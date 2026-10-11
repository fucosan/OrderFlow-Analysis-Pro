"""
MetaTrader 5 Data Feed — connects to MT5 terminal for real NAS100 & Gold tick data.

Provides:
  - Real-time tick polling from MT5 terminal (bid/ask/last, volume, buy/sell flags)
  - Historical tick/bar download for backtesting & VP computation
  - Book of market (DOM) data for orderbook analysis
  - Symbol info (tick size, contract size, session times)

Cross-platform:
  - Windows: uses the native `MetaTrader5` module (terminal running natively).
  - Linux:   uses the `mt5linux` RPC bridge — MT5 runs inside a Wine prefix and
             is exposed via `mt5server.exe` over a local socket. The bridge
             factory (`mt5_bridge.create_mt5_handle`) returns a drop-in handle
             with the same API on both platforms.

Requirements:
  - Windows: MetaTrader 5 terminal running + `pip install MetaTrader5`
  - Linux:   Wine-hosted MT5 + `mt5server.exe` running + `pip install mt5linux`
  - Broker account connected in MT5

MT5 Symbol Mapping (broker-dependent, adjust in config):
  - NAS100: "NAS100", "USTEC", "US100", "NAS100.cash", "USTEC.cash"
  - Gold:   "XAUUSD", "GOLD", "XAUUSD.cash"
"""

from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime, timezone, timedelta
from typing import Callable, Optional

from orderflow_system.data.mt5_bridge import create_mt5_handle, describe_backend
from orderflow_system.data.models import (
    Tick, Side, OrderbookSnapshot, OrderbookLevel, Candle,
)

logger = logging.getLogger(__name__)

# MT5 tick flags (from MetaTrader5 module constants)
TICK_FLAG_BID = 0x02
TICK_FLAG_ASK = 0x04
TICK_FLAG_LAST = 0x08
TICK_FLAG_VOLUME = 0x10
TICK_FLAG_BUY = 0x20
TICK_FLAG_SELL = 0x40


class MT5Feed:
    """
    Real-time and historical data feed from MetaTrader 5 terminal.

    Polling-based: MT5 Python API is synchronous, so we poll ticks in an
    async loop with configurable interval. For orderflow, we need the
    LAST price + BUY/SELL flags, not just bid/ask.

    Usage:
        feed = MT5Feed(
            symbols={"NAS100USDT": "USTEC", "XAUUSDT": "XAUUSD"},
            on_tick=my_tick_handler,
            on_orderbook=my_book_handler,
        )
        await feed.start()
    """

    def __init__(
        self,
        symbols: dict[str, str],  # {internal_name: mt5_symbol}
        on_tick: Optional[Callable] = None,
        on_orderbook: Optional[Callable] = None,
        poll_interval_ms: int = 100,
        enable_book: bool = True,
        login: int = 0,
        password: str = "",
        server: str = "",
        bridge_host: str = "127.0.0.1",
        bridge_port: int = 18812,
        bridge_timeout: int = 300,
    ):
        self.symbols = symbols  # e.g., {"NAS100USDT": "USTEC", "XAUUSDT": "XAUUSD"}
        self.on_tick = on_tick
        self.on_orderbook = on_orderbook
        self.poll_interval_ms = poll_interval_ms
        self.enable_book = enable_book
        self.login = login
        self.password = password
        self.server = server
        self.bridge_host = bridge_host
        self.bridge_port = bridge_port
        self.bridge_timeout = bridge_timeout
        self._running = False
        self._mt5 = None
        self._last_tick_time: dict[str, int] = {}  # Track last seen tick per symbol
        self._last_price: dict[str, float] = {}    # Last trade/mid price per symbol (tick rule)
        self._initialized = False

    def connect(self) -> bool:
        """Initialize MT5 connection (call before download_historical_ticks)."""
        if self._initialized:
            return True
        return self._initialize_mt5()

    async def start(self):
        """Initialize MT5 connection and begin polling."""
        if not self._initialized and not self._initialize_mt5():
            logger.error("Failed to initialize MT5. Make sure MT5 terminal is running.")
            return

        self._running = True

        # Enable market book for each symbol (DOM data)
        if self.enable_book:
            for internal, mt5_sym in self.symbols.items():
                self._mt5.market_book_add(mt5_sym)
                logger.info(f"Market book enabled for {mt5_sym}")

        logger.info(
            f"MT5 feed started. Polling {len(self.symbols)} symbols "
            f"every {self.poll_interval_ms}ms"
        )

        try:
            while self._running:
                await self._poll_cycle()
                await asyncio.sleep(self.poll_interval_ms / 1000.0)
        finally:
            await self.stop()

    async def stop(self):
        """Disconnect from MT5."""
        self._running = False
        if self._mt5 and self._initialized:
            if self.enable_book:
                for mt5_sym in self.symbols.values():
                    try:
                        self._mt5.market_book_release(mt5_sym)
                    except Exception:
                        pass
            self._mt5.shutdown()
            self._initialized = False
            logger.info("MT5 disconnected")

    def _initialize_mt5(self) -> bool:
        """Initialize MT5 connection (native module or mt5linux bridge)."""
        try:
            mt5 = create_mt5_handle(
                host=self.bridge_host,
                port=self.bridge_port,
                timeout=self.bridge_timeout,
                login=self.login or None,
                password=self.password or None,
                server=self.server or None,
            )
            self._mt5 = mt5
        except RuntimeError as exc:
            logger.error(str(exc))
            return False

        logger.info(f"MT5 backend: {describe_backend()}")

        # Build initialize kwargs. Both the native module (Windows) and the
        # mt5linux 0.x standalone client (Linux) accept login/password/server
        # here — the 0.x client forwards them to mt5.initialize() on the server.
        init_kwargs = {}
        if self.login:
            init_kwargs["login"] = self.login
        if self.password:
            init_kwargs["password"] = self.password
        if self.server:
            init_kwargs["server"] = self.server

        try:
            if not mt5.initialize(**init_kwargs):
                error = mt5.last_error()
                logger.error(f"MT5 initialize() failed: {error}")
                return False
        except Exception as exc:
            logger.error(f"MT5 initialize() raised: {exc}")
            return False

        self._initialized = True

        # Log account info
        try:
            account = mt5.account_info()
            if account:
                logger.info(
                    f"MT5 connected: {account.server} | "
                    f"Account: {account.login} | Balance: {account.balance}"
                )
        except Exception as exc:
            logger.warning(f"Could not read MT5 account info: {exc}")

        # Validate symbols exist
        for internal, mt5_sym in list(self.symbols.items()):
            info = mt5.symbol_info(mt5_sym)
            if info is None:
                logger.warning(
                    f"Symbol '{mt5_sym}' not found in MT5. "
                    f"Trying alternatives..."
                )
                # Try common alternatives
                found = self._find_symbol_alternative(mt5_sym, internal)
                if not found:
                    logger.error(
                        f"Could not find any matching symbol for {internal}. "
                        f"Available symbols can be listed with mt5.symbols_get()"
                    )
            else:
                if not info.visible:
                    mt5.symbol_select(mt5_sym, True)
                logger.info(
                    f"Symbol {mt5_sym} ({internal}): "
                    f"tick_size={info.trade_tick_size}, "
                    f"digits={info.digits}, "
                    f"spread={info.spread}"
                )

        return True

    # Known alternative symbol names per asset class (broker-dependent)
    _SYMBOL_ALTERNATIVES: dict[str, list[str]] = {
        # Indices
        "USTEC": ["USTEC", "USTECm", "USTECc", "NAS100", "US100", "USTEC.cash", "NAS100.cash", "USTECH100", "#NAS100", "NASDAQ"],
        "US500": ["US500", "US500m", "US500c", "SP500", "SPX500", "US500.cash", "#SP500", "SP500m"],
        "US30":  ["US30", "US30m", "US30c", "DJ30", "DJI30", "US30.cash", "#DJ30", "DJ30m"],
        "UK100": ["UK100", "UK100m", "UK100c", "FTSE100", "UK100.cash", "#UK100"],
        "DE30":  ["DE30", "DE30m", "DE30c", "DE40", "DE40m", "DAX40", "GER40", "GER30", "DE30.cash"],
        "JP225": ["JP225", "JP225m", "JP225c", "NI225", "NIKKEI225", "JP225.cash"],
        "FR40":  ["FR40", "FR40m", "FR40c", "CAC40", "FRA40", "FR40.cash"],
        "AUS200":["AUS200", "AUS200m", "AUS200c", "AU200", "ASX200", "AUS200.cash"],
        "HK50":  ["HK50", "HK50m", "HK50c", "HSI50", "HK50.cash"],
        # Metals
        "XAUUSD":["XAUUSD", "XAUUSDm", "XAUUSDc", "GOLD", "GOLDm", "XAUUSD.cash", "#XAUUSD"],
        "XAGUSD":["XAGUSD", "XAGUSDm", "XAGUSDc", "SILVER", "SILVERm", "XAGUSD.cash"],
        # Energy
        "USOIL": ["USOIL", "USOILm", "USOILc", "WTI", "XTIUSD", "XTIUSDm", "CrudeOIL", "USCrude"],
        "UKOIL": ["UKOIL", "UKOILm", "UKOILc", "BRENT", "XBRUSD", "XBRUSDm", "BrentOIL"],
        # Forex Majors
        "EURUSD":["EURUSD", "EURUSDm", "EURUSDc", "EURUSD.cash"],
        "GBPUSD":["GBPUSD", "GBPUSDm", "GBPUSDc"],
        "USDJPY":["USDJPY", "USDJPYm", "USDJPYc"],
        "AUDUSD":["AUDUSD", "AUDUSDm", "AUDUSDc"],
        "USDCAD":["USDCAD", "USDCADm", "USDCADc"],
        "USDCHF":["USDCHF", "USDCHFm", "USDCHFc"],
        "NZDUSD":["NZDUSD", "NZDUSDm", "NZDUSDc"],
        # Forex Crosses
        "EURGBP":["EURGBP", "EURGBPm", "EURGBPc"],
        "EURJPY":["EURJPY", "EURJPYm", "EURJPYc"],
        "GBPJPY":["GBPJPY", "GBPJPYm", "GBPJPYc"],
        # Stocks
        "AAPL":  ["AAPL", "AAPLm", "AAPLc", "#AAPL", "AAPL.US"],
        "TSLA":  ["TSLA", "TSLAm", "TSLAc", "#TSLA", "TSLA.US"],
        "AMZN":  ["AMZN", "AMZNm", "AMZNc", "#AMZN", "AMZN.US"],
        "MSFT":  ["MSFT", "MSFTm", "MSFTc", "#MSFT", "MSFT.US"],
        "NVDA":  ["NVDA", "NVDAm", "NVDAC", "#NVDA", "NVDA.US"],
        "META":  ["META", "METAm", "METAc", "#META", "META.US"],
        "GOOGL": ["GOOGL", "GOOGLm", "GOOGLc", "#GOOGL", "GOOGL.US", "GOOG", "GOOGm"],
        # Crypto
        "BTCUSD":["BTCUSD", "BTCUSDm", "BTCUSDc", "BTCUSDT", "BTCUSDTc"],
    }

    def _find_symbol_alternative(self, mt5_sym: str, internal: str) -> bool:
        """Try to find alternative symbol names for common instruments."""
        mt5 = self._mt5
        base = mt5_sym.upper().replace(".CASH", "").replace(".", "").rstrip("M")

        # Find matching alternatives list
        alternatives = []
        for key, alts in self._SYMBOL_ALTERNATIVES.items():
            if base == key.upper() or mt5_sym.upper().rstrip("M") == key.upper():
                alternatives = alts
                break

        # Fallback: try plain name with/without 'm'/'c' suffix
        if not alternatives:
            alternatives = [
                mt5_sym,
                mt5_sym.rstrip('m'),
                mt5_sym + 'm',
                mt5_sym.rstrip('c'),
                mt5_sym + 'c',
            ]

        for alt in alternatives:
            info = mt5.symbol_info(alt)
            if info is not None:
                if not info.visible:
                    mt5.symbol_select(alt, True)
                self.symbols[internal] = alt
                logger.info(f"Found alternative symbol: {alt} for {internal}")
                return True

        return False

    async def _poll_cycle(self):
        """Poll MT5 for new ticks and book data for all symbols."""
        mt5 = self._mt5

        for internal, mt5_sym in self.symbols.items():
            try:
                # ── Poll ticks ──
                await self._poll_ticks(internal, mt5_sym)

                # ── Poll orderbook (DOM) ──
                if self.enable_book and self.on_orderbook:
                    await self._poll_book(internal, mt5_sym)

            except Exception as e:
                logger.error(f"Error polling {mt5_sym}: {e}", exc_info=True)

    async def _poll_ticks(self, internal: str, mt5_sym: str):
        """
        Poll new ticks since last check.
        MT5 ticks have flags indicating BUY or SELL direction.
        """
        mt5 = self._mt5
        now = datetime.now(timezone.utc)

        if internal not in self._last_tick_time:
            # First poll — get ticks from last 2 seconds
            from_dt = now - timedelta(seconds=2)
        else:
            # Get ticks since last poll
            from_dt = datetime.fromtimestamp(
                self._last_tick_time[internal] / 1000.0, tz=timezone.utc
            )

        # copy_ticks_from returns numpy array of ticks
        ticks_data = mt5.copy_ticks_from(mt5_sym, from_dt, 1000, mt5.COPY_TICKS_ALL)

        if ticks_data is None or len(ticks_data) == 0:
            return

        for t in ticks_data:
            # Skip if we already processed this tick
            tick_time_ms = int(t['time_msc'])
            if internal in self._last_tick_time and tick_time_ms <= self._last_tick_time[internal]:
                continue

            # Determine aggressor side from tick flags
            flags = int(t['flags'])
            last_price = float(t['last'])
            bid = float(t['bid'])
            ask = float(t['ask'])

            if flags & TICK_FLAG_BUY:
                side = Side.BUY
            elif flags & TICK_FLAG_SELL:
                side = Side.SELL
            elif last_price > 0:
                # Trade tick without a buy/sell flag — use price vs bid/ask.
                if last_price >= ask:
                    side = Side.BUY
                elif last_price <= bid:
                    side = Side.SELL
                else:
                    side = Side.BUY  # inside spread → ambiguous
            else:
                # QUOTE-ONLY tick (no trade price, e.g. Exness BTCUSDc sends
                # only bid/ask updates with flags=0x06). Classify with the
                # classic TICK RULE: compare the mid to the previous mid —
                # uptick = buyer-initiated, downtick = seller-initiated.
                mid = (bid + ask) / 2.0 if (bid > 0 and ask > 0) else 0.0
                prev = self._last_price.get(internal, 0.0)
                if mid > 0 and prev > 0:
                    if mid > prev:
                        side = Side.BUY
                    elif mid < prev:
                        side = Side.SELL
                    else:
                        side = Side.BUY  # unchanged → neutral, default buy
                else:
                    side = Side.BUY  # no reference yet
                if mid > 0:
                    self._last_price[internal] = mid

            # Use 'last' price (actual trade price) when available,
            # fall back to mid of bid/ask
            if last_price == 0:
                last_price = (bid + ask) / 2.0

            volume = float(t['volume_real']) if t['volume_real'] > 0 else float(t['volume'])
            if volume == 0:
                volume = 1.0  # Some brokers don't provide real volume

            tick = Tick(
                timestamp_ms=tick_time_ms,
                price=last_price,
                size=volume,
                side=side,
                trade_id=f"mt5_{tick_time_ms}",
            )

            if self.on_tick:
                await self.on_tick(internal, tick)

        # Update last tick time
        self._last_tick_time[internal] = int(ticks_data[-1]['time_msc'])

    async def _poll_book(self, internal: str, mt5_sym: str):
        """
        Poll the order book (DOM / Market Depth) from MT5.
        Converts MT5 book entries to our OrderbookSnapshot model.
        """
        mt5 = self._mt5
        book = mt5.market_book_get(mt5_sym)

        if book is None or len(book) == 0:
            return

        bids = []
        asks = []
        now_ms = int(time.time() * 1000)

        for entry in book:
            level = OrderbookLevel(
                price=entry.price,
                quantity=float(entry.volume_real if entry.volume_real > 0 else entry.volume),
            )
            # MT5 book type: 1 = SELL (ask side), 2 = BUY (bid side)
            if entry.type == 1:  # BOOK_TYPE_SELL
                asks.append(level)
            elif entry.type == 2:  # BOOK_TYPE_BUY
                bids.append(level)

        snapshot = OrderbookSnapshot(
            timestamp_ms=now_ms,
            bids=sorted(bids, key=lambda x: -x.price),
            asks=sorted(asks, key=lambda x: x.price),
        )

        if self.on_orderbook:
            await self.on_orderbook(internal, snapshot)

    # ── Historical Data Methods ──

    async def download_historical_ticks(
        self,
        mt5_sym: str,
        from_date: datetime,
        to_date: datetime,
    ) -> list[Tick]:
        """
        Download historical ticks from MT5 for backtesting.
        Uses copy_ticks_range() which can return millions of ticks.
        """
        mt5 = self._mt5
        if not self._initialized:
            self._initialize_mt5()

        logger.info(f"Downloading ticks for {mt5_sym} from {from_date} to {to_date}")

        ticks_data = mt5.copy_ticks_range(
            mt5_sym, from_date, to_date, mt5.COPY_TICKS_ALL
        )

        if ticks_data is None or len(ticks_data) == 0:
            logger.warning(f"No ticks returned for {mt5_sym}")
            return []

        ticks = []
        for t in ticks_data:
            flags = int(t['flags'])
            if flags & TICK_FLAG_BUY:
                side = Side.BUY
            elif flags & TICK_FLAG_SELL:
                side = Side.SELL
            else:
                last_price = float(t['last'])
                bid = float(t['bid'])
                ask = float(t['ask'])
                side = Side.BUY if last_price >= ask else Side.SELL

            last_price = float(t['last'])
            if last_price == 0:
                last_price = (float(t['bid']) + float(t['ask'])) / 2.0

            volume = float(t['volume_real']) if t['volume_real'] > 0 else float(t['volume'])
            if volume == 0:
                volume = 1.0

            ticks.append(Tick(
                timestamp_ms=int(t['time_msc']),
                price=last_price,
                size=volume,
                side=side,
                trade_id=f"mt5_{t['time_msc']}",
            ))

        logger.info(f"Downloaded {len(ticks)} ticks for {mt5_sym}")
        return ticks

    async def download_historical_candles(
        self,
        mt5_sym: str,
        timeframe: int,  # MT5 timeframe constant (e.g., mt5.TIMEFRAME_M1)
        from_date: datetime,
        to_date: datetime,
    ) -> list[Candle]:
        """
        Download historical OHLCV bars from MT5.
        Note: MT5 bars don't have buy/sell volume split — only total.
        """
        mt5 = self._mt5
        if not self._initialized:
            self._initialize_mt5()

        rates = mt5.copy_rates_range(mt5_sym, timeframe, from_date, to_date)

        if rates is None or len(rates) == 0:
            logger.warning(f"No bars returned for {mt5_sym}")
            return []

        candles = []
        for r in rates:
            candles.append(Candle(
                timestamp_ms=int(r['time']) * 1000,
                open=float(r['open']),
                high=float(r['high']),
                low=float(r['low']),
                close=float(r['close']),
                volume=float(r['real_volume'] if r['real_volume'] > 0 else r['tick_volume']),
                buy_volume=0.0,   # MT5 bars don't split buy/sell
                sell_volume=0.0,
                tick_count=int(r['tick_volume']),
            ))

        logger.info(f"Downloaded {len(candles)} bars for {mt5_sym}")
        return candles

    def get_symbol_info(self, mt5_sym: str) -> Optional[dict]:
        """Get symbol properties from MT5."""
        mt5 = self._mt5
        info = mt5.symbol_info(mt5_sym)
        if info is None:
            return None
        return {
            "name": info.name,
            "description": info.description,
            "tick_size": info.trade_tick_size,
            "tick_value": info.trade_tick_value,
            "digits": info.digits,
            "spread": info.spread,
            "contract_size": info.trade_contract_size,
            "volume_min": info.volume_min,
            "volume_max": info.volume_max,
            "volume_step": info.volume_step,
            "currency_base": info.currency_base,
            "currency_profit": info.currency_profit,
        }

    # ── Order Execution (auto-trading) ──
    # These wrap the MT5 trade API. The request dict keys follow the native
    # MetaTrader5 `order_send` request schema. The underlying handle is either
    # the native module (Windows) or the RPyC bridge to Wine-hosted MT5
    # (Linux), both of which expose the same API.

    def get_mt5_symbol(self, internal: str) -> Optional[str]:
        """Resolve an internal name (BTCUSDT) to the broker symbol (BTCUSDm)."""
        return self.symbols.get(internal)

    def order_send(self, request: dict):
        """Send a trade request and return the raw MT5 result dict."""
        mt5 = self._mt5
        if mt5 is None:
            return None
        try:
            result = mt5.order_send(request)
            if result is None:
                return None
            return {
                "retcode": getattr(result, "retcode", None),
                "deal": getattr(result, "deal", 0),
                "order": getattr(result, "order", 0),
                "volume": getattr(result, "volume", 0),
                "price": getattr(result, "price", 0),
                "request": result.request,
            }
        except Exception as exc:
            logger.error(f"MT5 order_send failed: {exc}")
            return None

    def precision(self, internal: str) -> int:
        """Return the price digits for a symbol (default 2)."""
        mt5_sym = self.get_mt5_symbol(internal)
        info = mt5_sym and self._mt5 and self._mt5.symbol_info(mt5_sym)
        if info is None:
            return 2
        return int(getattr(info, "digits", 2))

    def list_available_symbols(self, filter_text: str = "") -> list[str]:
        """List available symbols in MT5 matching a filter."""
        mt5 = self._mt5
        if filter_text:
            symbols = mt5.symbols_get(filter_text)
        else:
            symbols = mt5.symbols_get()
        if symbols is None:
            return []
        return [s.name for s in symbols]
