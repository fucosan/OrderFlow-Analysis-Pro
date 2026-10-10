"""
Interactive Brokers (IB Gateway) data feed via ib_async.

Connects to a locally running IB Gateway / TWS (default port 4002 for IB
Gateway paper, 4001 for live; TWS uses 7497/7496). Provides real-time
tick-by-tick trades with aggressor-side inference from the prevailing
bid/ask, plus historical bars for backtesting and volume-profile rebuilds.

Requirements:
  - IB Gateway or TWS running locally with API enabled
  - pip install ib_async

Notes on aggressor side:
  IB does not expose the taker/buy-sell flag directly on tick-by-tick
  "Last" data. We infer it from the prevailing bid/ask (last >= ask → BUY,
  last <= bid → SELL), matching the fallback heuristic used by the MT5 feed.
"""

from __future__ import annotations

import asyncio
import logging
import threading
from typing import Callable, Optional

from ib_async import IB, Contract, Stock, Forex, CFD, Index, Crypto

from orderflow_system.data.models import Tick, Side, Candle

logger = logging.getLogger(__name__)


def build_contract(spec: tuple) -> Contract:
    """
    Build an ib_async Contract from a (type, symbol, currency, exchange) spec.

    Examples:
      ("CFD",    "XAUUSD", "USD", "SMART")  → gold spot CFD
      ("CRYPTO", "BTC",    "USD", "PAXOS")  → bitcoin
      ("STOCK",  "AAPL",   "USD", "SMART")  → US stock
      ("FOREX",  "EURUSD", "USD", "IDEALPRO")
      ("INDEX",  "SPX",    "USD", "CBOE")
    """
    ctype, symbol, currency, exchange = spec
    ctype = ctype.upper()
    if ctype == "STOCK":
        return Stock(symbol, exchange, currency)
    if ctype == "FOREX":
        return Forex(symbol)
    if ctype == "CFD":
        return CFD(symbol, exchange, currency)
    if ctype == "INDEX":
        return Index(symbol, exchange, currency)
    if ctype == "CRYPTO":
        return Crypto(symbol, exchange, currency)
    raise ValueError(f"Unknown IB contract type: {ctype}")


class IBFeed:
    """
    Real-time data feed from Interactive Brokers via IB Gateway.

    Subscribes to:
      - tick-by-tick "Last"  → executed trades (price + size)
      - market data (bid/ask) → used to infer the aggressor side

    The IB event loop runs in a background thread (ib_async's `run()`),
    and async callbacks are bridged back onto the main asyncio loop.
    """

    def __init__(
        self,
        symbols: dict[str, tuple],  # {internal_name: contract spec}
        on_tick: Optional[Callable] = None,
        on_orderbook: Optional[Callable] = None,
        host: str = "127.0.0.1",
        port: int = 4002,
        client_id: int = 1,
    ):
        self.symbols = symbols
        self.on_tick = on_tick
        self.on_orderbook = on_orderbook
        self.host = host
        self.port = port
        self.client_id = client_id

        self._ib: Optional[IB] = None
        self._running = False
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._thread: Optional[threading.Thread] = None

        self._contracts: dict[str, Contract] = {}
        self._contract_to_internal: dict[Contract, str] = {}

        self._last_bid: dict[str, float] = {}
        self._last_ask: dict[str, float] = {}
        self._last_tick_time: dict[str, int] = {}

    # ── Lifecycle ──

    async def start(self):
        """Connect to IB Gateway and begin receiving data."""
        self._loop = asyncio.get_running_loop()
        self._running = True

        self._ib = IB()
        try:
            await self._ib.connectAsync(
                self.host, self.port, clientId=self.client_id
            )
        except Exception as e:
            logger.error(
                f"IB connect failed to {self.host}:{self.port}: {e}. "
                f"Is IB Gateway running with API enabled?"
            )
            self._running = False
            return

        logger.info(f"Connected to IB Gateway at {self.host}:{self.port}")

        # Run the IB event loop in a background thread.
        self._thread = threading.Thread(target=self._ib.run, daemon=True)
        self._thread.start()

        # Wire event handlers.
        self._ib.pendingTickersEvent += self._on_pending_tickers

        # Subscribe to each symbol.
        for internal, spec in self.symbols.items():
            contract = build_contract(spec)

            # Qualify the contract to populate its conId (required before
            # it can be hashed / used as a dict key and before subscribing).
            try:
                qualified = await self._ib.qualifyContractsAsync(contract)
            except Exception as e:
                logger.error(f"[{internal}] Contract qualification failed: {e}")
                continue
            if not qualified:
                logger.error(
                    f"[{internal}] Could not qualify contract "
                    f"{contract.symbol} / {contract.secType}"
                )
                continue
            contract = qualified[0]

            self._contracts[internal] = contract
            self._contract_to_internal[contract] = internal

            # Tick-by-tick "AllLast" delivers executed trades (price + size).
            self._ib.reqTickByTickData(contract, "AllLast", 0, False)
            # Market data for bid/ask so we can infer the aggressor side.
            self._ib.reqMktData(contract, "", False, False)

            logger.info(
                f"Subscribed to {internal} "
                f"({contract.symbol} / {contract.secType})"
            )

        # Keep the coroutine alive while the feed runs.
        while self._running:
            await asyncio.sleep(1)

    async def stop(self):
        """Disconnect from IB Gateway."""
        self._running = False
        if self._ib:
            try:
                self._ib.disconnect()
            except Exception:
                pass
        if self._thread:
            self._thread.join(timeout=2)
        logger.info("IB feed disconnected")

    # ── IB event handlers (run on the IB background thread) ──

    def _on_pending_tickers(self, tickers):
        """
        Handle batched ticker updates from the IB event loop.

        Each ticker carries:
          - tickByTicks: list of TickByTickAllLast (executed trades)
          - bid / ask:   prevailing quotes used to infer aggressor side
        """
        for t in tickers:
            internal = self._contract_to_internal.get(t.contract)
            if not internal:
                continue

            # Track latest bid/ask for aggressor-side inference.
            if t.bid is not None:
                self._last_bid[internal] = float(t.bid)
            if t.ask is not None:
                self._last_ask[internal] = float(t.ask)

            # Process executed trades.
            for tick in t.tickByTicks:
                if tick.size is None or tick.size <= 0:
                    continue
                asyncio.run_coroutine_threadsafe(
                    self._process_tick(
                        internal, tick.time, tick.price, tick.size
                    ),
                    self._loop,
                )

    # ── Tick processing (runs on the main asyncio loop) ──

    async def _process_tick(self, internal: str, time, price, size):
        ts_ms = int(time.timestamp() * 1000)

        # Skip duplicates / out-of-order ticks.
        if internal in self._last_tick_time and ts_ms <= self._last_tick_time[internal]:
            return
        self._last_tick_time[internal] = ts_ms

        # Infer aggressor side from prevailing bid/ask.
        bid = self._last_bid.get(internal)
        ask = self._last_ask.get(internal)
        if ask is not None and price >= ask:
            side = Side.BUY
        elif bid is not None and price <= bid:
            side = Side.SELL
        else:
            side = Side.BUY  # Default to buy if ambiguous

        tick = Tick(
            timestamp_ms=ts_ms,
            price=float(price),
            size=float(size),
            side=side,
            trade_id=f"ib_{ts_ms}",
        )

        if self.on_tick:
            await self.on_tick(internal, tick)

    # ── Historical data ──

    async def download_historical_candles(
        self,
        internal: str,
        timeframe: str = "1 min",
        days: int = 3,
    ) -> list[Candle]:
        """
        Download historical OHLCV bars from IB for the given instrument.

        timeframe uses IB bar-size strings, e.g. "1 min", "5 mins", "1 hour".
        """
        contract = self._contracts.get(internal)
        if not contract:
            logger.warning(f"[{internal}] No IB contract configured")
            return []

        # Crypto requires AGGTRADES; everything else uses TRADES.
        what_to_show = "AGGTRADES" if contract.secType == "CRYPTO" else "TRADES"

        duration = f"{days} D"
        try:
            bars = await self._ib.reqHistoricalDataAsync(
                contract,
                endDateTime="",
                durationStr=duration,
                barSizeSetting=timeframe,
                whatToShow=what_to_show,
                useRTH=False,
                formatDate=2,
            )
        except Exception as e:
            logger.error(f"[{internal}] IB history download failed: {e}")
            return []

        if not bars:
            logger.warning(f"[{internal}] No historical bars returned")
            return []

        candles = []
        for b in bars:
            candles.append(Candle(
                timestamp_ms=int(b.date.timestamp() * 1000),
                open=float(b.open),
                high=float(b.high),
                low=float(b.low),
                close=float(b.close),
                volume=float(b.volume),
                buy_volume=0.0,   # IB bars don't split buy/sell
                sell_volume=0.0,
                tick_count=int(b.barCount),
            ))

        logger.info(f"[{internal}] Downloaded {len(candles)} bars from IB")
        return candles

    async def download_history(
        self,
        pipelines: dict,
        timeframe: str = "1 min",
        days: int = 3,
    ) -> int:
        """
        Download historical bars for all configured symbols and inject them
        into the given pipelines' candle builders (mirrors MT5 warmup).

        Returns the total number of candles loaded.
        """
        from datetime import datetime, timezone

        total_added = 0
        now = datetime.now(timezone.utc)

        for internal in self.symbols:
            pipeline = pipelines.get(internal)
            if not pipeline:
                continue

            try:
                candles = await self.download_historical_candles(
                    internal, timeframe=timeframe, days=days
                )
                if not candles:
                    continue

                added = pipeline.candle_builder.load_historical_candles(candles)
                total_added += added

                # Build VP from candle history
                today = now.strftime("%Y-%m-%d")
                all_candles = pipeline.candle_builder.history
                if all_candles:
                    vp = pipeline.vp_engine.compute_from_candles(
                        all_candles, session_date=today
                    )
                    if vp.total_volume > 0:
                        pipeline.profile_framing.add_profile(vp)

                logger.info(
                    f"[{internal}] +{added} bars (total {len(all_candles)})"
                )

            except Exception as e:
                logger.error(
                    f"[{internal}] IB history download failed: {e}",
                    exc_info=True,
                )

        logger.info(
            f"IB history download complete: {total_added} total candles loaded"
        )
        return total_added
