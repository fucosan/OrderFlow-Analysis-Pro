"""
Main Orchestrator — Wires all components together and runs the system.

Architecture:
  Data Source (MT5 or Bybit) → Ticks → CandleBuilder → Analytics Engines → Pattern Detectors
       ↓                              ↓                        ↓
  OrderbookTracker              DeltaEngine              Profile Framing
       ↓                              ↓                        ↓
                       Signal Aggregator (State Machine)
                                    ↓
                       Telegram Alert Bot + Database Logging

Data Sources:
  - MT5 (default): Real NAS100/XAUUSD data from MetaTrader 5 terminal
  - Bybit: Free perpetual futures data via WebSocket
  - Both: Run both feeds simultaneously
"""

from __future__ import annotations

import asyncio
import logging
import os
import signal
import sys
from datetime import datetime, timedelta, timezone
from typing import Optional

from orderflow_system.config.settings import (
    InstrumentConfig,
    Instrument,
    DataSource,
    SessionType,
    get_all_configs,
    TELEGRAM,
    DB_PATH,
    LOG_LEVEL,
    DATA_SOURCE,
    MT5,
    IB,
    DASHBOARD,
)
from orderflow_system.data.models import (
    Tick, Candle, Signal, Side, OrderbookSnapshot,
)
from orderflow_system.data.bybit_feed import BybitFeed
from orderflow_system.data.mt5_feed import MT5Feed
from orderflow_system.data.ib_feed import IBFeed
from orderflow_system.data.candle_builder import CandleBuilder
from orderflow_system.data.database import Database
from orderflow_system.analytics.volume_profile import VolumeProfileEngine
from orderflow_system.analytics.delta import DeltaEngine
from orderflow_system.analytics.footprint import FootprintEngine
from orderflow_system.analytics.orderbook import OrderbookTracker
from orderflow_system.patterns.absorption import AbsorptionDetector
from orderflow_system.patterns.initiative import InitiativeDetector
from orderflow_system.patterns.sweep import SweepDetector
from orderflow_system.patterns.exhaustion import ExhaustionDetector
from orderflow_system.patterns.divergence import DivergenceDetector
from orderflow_system.signals.profile_framing import ProfileFramingEngine
from orderflow_system.signals.aggregator import SignalAggregator
from orderflow_system.execution.mt5_executor import MT5ExecutionHandler
from orderflow_system.alerts.telegram_bot import TelegramAlertBot
from orderflow_system.dashboard.websocket_manager import WebSocketManager
from orderflow_system.dashboard.app import app as dashboard_app, set_system, ws_manager

logger = logging.getLogger("orderflow_system")


# Session windows in UTC hours (start_hour, end_hour). End hour is exclusive.
# Used to build session-aligned daily volume profiles (e.g. gold's primary
# liquidity window = London + NY overlap).
SESSION_WINDOWS_UTC: dict[SessionType, tuple[int, int]] = {
    SessionType.FULL_DAY: (0, 24),
    SessionType.ASIAN: (0, 9),
    SessionType.LONDON: (7, 17),           # London+NY overlap: 07:00–17:00 UTC
    SessionType.NY_CASH: (13, 21),          # 09:30–16:00 ET ≈ 13:30–20:00 UTC
}


def _session_window_utc(session: SessionType) -> tuple[int, int]:
    """Return the UTC (start_hour, end_hour) window for a session type.

    Falls back to full-day if the type isn't mapped.
    """
    return SESSION_WINDOWS_UTC.get(session, (0, 24))


def _session_start_end_ms(
    now: datetime, session: SessionType
) -> tuple[int, int]:
    """
    Compute the [start_ms, end_ms] window for the current/active session.

    - FULL_DAY → today's 00:00 UTC up to now.
    - If "now" is inside the session window → today's start → now.
    - If "now" is past the session end → today's full session (start → end).
    - If "now" is before the session start → use YESTERDAY's completed session.
    """
    if session == SessionType.FULL_DAY:
        day = now.replace(hour=0, minute=0, second=0, microsecond=0)
        return int(day.timestamp() * 1000), int(now.timestamp() * 1000)

    start_hour, end_hour = _session_window_utc(session)
    day = now.replace(hour=0, minute=0, second=0, microsecond=0)
    start_today = day.replace(hour=start_hour)
    end_today = day.replace(hour=end_hour)

    now_ts = now.timestamp()
    if now >= end_today:
        # Session finished today → use today's full window (start → end)
        return int(start_today.timestamp() * 1000), int(end_today.timestamp() * 1000)
    if now >= start_today:
        # Session in progress → start → now
        return int(start_today.timestamp() * 1000), int(now_ts * 1000)
    # Before today's session → use yesterday's completed session
    prev_start = start_today - timedelta(days=1)
    prev_end = end_today - timedelta(days=1)
    return int(prev_start.timestamp() * 1000), int(prev_end.timestamp() * 1000)


def _filter_candles_by_session(
    candles, session: SessionType
) -> list:
    """Filter a list of candles to only those falling within `session`'s UTC
    window for each candle's own day.

    - FULL_DAY → all candles kept.
    - e.g. gold LONDON (07:00–17:00 UTC) → keeps only candle-hours in 7–16.
    """
    if session == SessionType.FULL_DAY:
        return list(candles)

    start_hour, end_hour = _session_window_utc(session)
    kept = []
    for c in candles:
        dt = datetime.fromtimestamp(c.timestamp_ms / 1000, tz=timezone.utc)
        hour = dt.hour
        if start_hour <= hour < end_hour:
            kept.append(c)
    return kept


class InstrumentPipeline:
    """
    Full processing pipeline for a single instrument.
    Ticks → Candles → Analytics → Patterns → Signals → Alerts.
    """

    def __init__(self, config: InstrumentConfig):
        self.config = config
        self.symbol = config.instrument.value

        # Analytics engines
        self.candle_builder = CandleBuilder(
            interval_seconds=60,
            tick_size=config.tick_size,
            on_candle_close=self._on_candle_close,
        )
        self.vp_engine = VolumeProfileEngine(config.volume_profile)
        self.delta_engine = DeltaEngine(tick_size=config.tick_size)
        self.footprint_engine = FootprintEngine(tick_size=config.tick_size)
        self.orderbook_tracker = OrderbookTracker(
            thin_threshold=config.sweep.thin_book_threshold
        )

        # Pattern detectors
        self.absorption = AbsorptionDetector(config.absorption, tick_size=config.tick_size)
        self.initiative = InitiativeDetector(config.initiative, tick_size=config.tick_size)
        self.sweep = SweepDetector(config.sweep)
        self.exhaustion = ExhaustionDetector(config.exhaustion)
        self.divergence = DivergenceDetector(config.divergence)

        # Profile framing
        self.profile_framing = ProfileFramingEngine()

        # Current price tracker
        self._current_price: float = 0.0
        self._tick_count: int = 0
        # Callback for routing candle-close signals to the system
        self._on_signals_callback = None
        # Callback for routing closed candles to the system (DB save + broadcast)
        self._on_candle_close_callback = None
        self._candle_count: int = 0

    async def process_tick(self, tick: Tick):
        """Process a single tick through the pipeline."""
        self._current_price = tick.price
        self._tick_count += 1
        await self.candle_builder.process_tick(tick)

    async def process_orderbook(self, snapshot: OrderbookSnapshot):
        """Process an orderbook update."""
        book_state = self.orderbook_tracker.update(snapshot)

        # Check for sweep on every book update
        current_candle = self.candle_builder.current_candle
        if current_candle and self.footprint_engine.history:
            fp = self.footprint_engine.history[-1]
            signal = self.sweep.check(
                self.orderbook_tracker, current_candle, fp
            )
            if signal:
                return signal
        return None

    async def _on_candle_close(self, candle: Candle) -> list[Signal]:
        """
        Called when a candle closes. Run all analytics and pattern checks.
        This is the main processing pipeline for each candle.
        """
        self._candle_count += 1
        signals: list[Signal] = []

        # 1. Compute delta
        delta = self.delta_engine.compute_from_candle(candle)

        # 2. Build footprint
        footprint = self.footprint_engine.build_from_candle(candle)

        # 3. Check each pattern detector
        # Absorption
        abs_signal = self.absorption.check_candle(
            candle, footprint, delta, self._current_price
        )
        if abs_signal:
            signals.append(abs_signal)
            logger.info(f"[{self.symbol}] {abs_signal}")

        # Initiative
        init_signal = self.initiative.check_candle(candle, delta, footprint)
        if init_signal:
            signals.append(init_signal)
            logger.info(f"[{self.symbol}] {init_signal}")

        # Exhaustion (needs candle history)
        recent = self.candle_builder.get_recent_candles(10)
        exh_signal = self.exhaustion.check_candle(
            candle, delta, self.delta_engine, footprint, recent
        )
        if exh_signal:
            signals.append(exh_signal)
            logger.info(f"[{self.symbol}] {exh_signal}")

        # Divergence
        div_signal = self.divergence.check_candle(candle, self.delta_engine)
        if div_signal:
            signals.append(div_signal)
            logger.info(f"[{self.symbol}] {div_signal}")

        # Route signals to the system if callback is set
        if signals and self._on_signals_callback:
            await self._on_signals_callback(self.symbol, signals)

        # Route the closed candle to the system (DB save + broadcast)
        if self._on_candle_close_callback:
            await self._on_candle_close_callback(self.symbol, candle)

        return signals

    @property
    def current_price(self) -> float:
        return self._current_price

    @property
    def stats(self) -> dict:
        return {
            "symbol": self.symbol,
            "price": self._current_price,
            "ticks": self._tick_count,
            "candles": self._candle_count,
            "cum_delta": self.delta_engine.cumulative_delta,
        }


class OrderflowSystem:
    """
    Main system orchestrator.
    Manages multiple instruments, coordinates between pipelines,
    and routes signals to alerts.
    """

    def __init__(
        self,
        instruments: Optional[list[InstrumentConfig]] = None,
        data_source: DataSource = DATA_SOURCE,
    ):
        if instruments is None:
            instruments = get_all_configs()

        self.data_source = data_source
        self.pipelines: dict[str, InstrumentPipeline] = {}
        for cfg in instruments:
            self.pipelines[cfg.instrument.value] = InstrumentPipeline(cfg)

        self.aggregator = SignalAggregator(
            min_composite_score=40.0,
            signal_cooldown_seconds=30.0,
        )

        # Per-instrument minimum SL distance (price units) from config.
        for cfg in instruments:
            if cfg.risk.min_sl_distance > 0:
                self.aggregator.set_min_sl_distance(
                    cfg.instrument.value, cfg.risk.min_sl_distance
                )

        # Wire candle-close signals from each pipeline back to the system
        for sym, pipeline in self.pipelines.items():
            pipeline._on_signals_callback = self._on_candle_signals
            pipeline._on_candle_close_callback = self._on_candle_close_handler
        self.telegram = TelegramAlertBot(
            bot_token=TELEGRAM.bot_token,
            chat_id=TELEGRAM.chat_id,
        )
        self.db = Database(DB_PATH)
        self.feed: Optional[BybitFeed] = None
        self.mt5_feed: Optional[MT5Feed] = None
        self.ib_feed: Optional[IBFeed] = None
        # Automated MT5 execution (disabled unless MT5_AUTO_TRADE=1).
        self.executor: Optional[MT5ExecutionHandler] = None
        self.ws_manager: WebSocketManager = ws_manager
        self._running = False
        self._tick_batch_size = 100
        self._tick_buffers: dict[str, list[Tick]] = {}

    async def start(self):
        """Start the complete system."""
        logger.info("=" * 60)
        logger.info("  ORDERFLOW TRADING ALERT SYSTEM")
        logger.info("  Based on Fabio's Orderflow Methodology")
        logger.info("=" * 60)

        # Initialize database
        await self.db.connect()
        logger.info("Database connected")

        # Initialize Telegram
        await self.telegram.initialize()

        # Load historical profiles (if available) into profile framing engines
        for symbol, pipeline in self.pipelines.items():
            profiles = await self.db.get_volume_profiles(symbol, days=5)
            for vp in profiles:
                pipeline.profile_framing.add_profile(vp)
            if profiles:
                bias = pipeline.profile_framing.analyze(pipeline.current_price)
                logger.info(
                    f"[{symbol}] Loaded {len(profiles)} historical profiles. "
                    f"Bias: {bias.direction.value} ({bias.confidence:.0f}%)"
                )
                await self.telegram.send_daily_bias_update(symbol, bias)

        # Start data feed(s) based on configured source
        symbols = list(self.pipelines.keys())
        feed_tasks = []

        if self.data_source in (DataSource.MT5, DataSource.BOTH):
            # ── MetaTrader 5 Feed ──
            # In BOTH mode, MT5 is used ONLY for historical warmup (it has
            # history but no real orderflow on Exness). Bybit provides the
            # live orderflow feed. Nulling the live callbacks prevents MT5
            # ticks from double-counting into the same candle builder.
            mt5_live = self.data_source == DataSource.MT5
            self.mt5_feed = MT5Feed(
                symbols=dict(MT5.symbols),
                on_tick=self._on_tick if mt5_live else None,
                on_orderbook=self._on_orderbook if mt5_live else None,
                poll_interval_ms=MT5.poll_interval_ms,
                enable_book=MT5.enable_book and mt5_live,
                login=MT5.login,
                password=MT5.password,
                server=MT5.server,
                bridge_host=MT5.bridge_host,
                bridge_port=MT5.bridge_port,
                bridge_timeout=MT5.bridge_timeout,
            )
            feed_tasks.append(self.mt5_feed.start())
            logger.info(
                f"MT5 feed configured: {len(MT5.symbols)} symbols "
                f"(poll: {MT5.poll_interval_ms}ms, book: {MT5.enable_book})"
            )

            # Connect to MT5 early so symbol validation happens before feeds start
            if not self.mt5_feed.connect():
                logger.error("MT5 connection failed — skipping history download")
            elif MT5.download_history_days > 0:
                # Download M1 bars synchronously BEFORE starting feeds/dashboard
                self._download_mt5_history_sync()

            # Attach the automated execution handler to the MT5 feed. It reuses
            # the feed's live handle (native Windows module or Wine RPyC bridge),
            # so auto orders go to the SAME terminal providing tick data.
            # Per-instrument allowlist: only instruments whose config has
            # risk.auto_trade=True may be auto-traded (plus the global gate).
            auto_instruments = {
                sym
                for sym, pipeline in self.pipelines.items()
                if pipeline.config.risk.auto_trade
            }
            # Optional env override: MT5_AUTO_TRADE_SYMBOLS restricts further.
            env_syms = {
                s.strip().upper()
                for s in os.getenv("MT5_AUTO_TRADE_SYMBOLS", "").split(",")
                if s.strip()
            }
            if env_syms:
                auto_instruments &= env_syms
            self.executor = MT5ExecutionHandler(
                self.mt5_feed, instruments=auto_instruments
            )
            if self.executor.enabled:
                allowed = self.executor.get_auto_instruments()
                logger.info(
                    "✅ MT5 AUTO-TRADING ENABLED (MT5_AUTO_TRADE=1). "
                    f"Auto-trade instruments: "
                    f"{', '.join(sorted(allowed)) if allowed else 'ALL'}. "
                    "Ensure 'Algo Trading' is on in the MT5 terminal."
                )
            else:
                logger.info(
                    "MT5 auto-trading DISABLED (default). Set MT5_AUTO_TRADE=1 "
                    "in .env to let the system place orders automatically."
                )

        if self.data_source in (DataSource.BYBIT, DataSource.BOTH):
            # ── Bybit WebSocket Feed ──
            self.feed = BybitFeed(
                symbols=symbols,
                on_tick=self._on_tick,
                on_orderbook=self._on_orderbook,
            )
            feed_tasks.append(self.feed.start())
            logger.info(f"Bybit feed configured: {symbols}")

        if self.data_source in (DataSource.IB, DataSource.BOTH):
            # ── Interactive Brokers Feed ──
            self.ib_feed = IBFeed(
                symbols=dict(IB.symbols),
                on_tick=self._on_tick,
                on_orderbook=self._on_orderbook,
                host=IB.host,
                port=IB.port,
                client_id=IB.client_id,
            )
            feed_tasks.append(self.ib_feed.start())
            logger.info(
                f"IB feed configured: {len(IB.symbols)} symbols "
                f"({IB.host}:{IB.port})"
            )

            # Download historical bars once the feed has connected/qualified.
            if IB.download_history_days > 0:
                feed_tasks.append(
                    self._download_ib_history(IB.download_history_days)
                )

        self._running = True
        source_name = self.data_source.value.upper()
        logger.info(f"Starting live feed [{source_name}] for: {', '.join(symbols)}")

        # ── Dashboard (FastAPI + Uvicorn) ──
        if DASHBOARD.enabled:
            import uvicorn
            set_system(self)
            uvi_config = uvicorn.Config(
                dashboard_app,
                host=DASHBOARD.host,
                port=DASHBOARD.port,
                log_level=DASHBOARD.log_level,
                loop="none",
            )
            uvi_server = uvicorn.Server(uvi_config)
            feed_tasks.append(uvi_server.serve())
            logger.info(f"Dashboard enabled at http://{DASHBOARD.host}:{DASHBOARD.port}")

        # Initial volume profile rebuild (once enough candles accumulate)
        for symbol, pipeline in self.pipelines.items():
            await self._rebuild_volume_profile(symbol, pipeline)

        # Run feed(s) + periodic tasks + dashboard concurrently
        await asyncio.gather(
            *feed_tasks,
            self._periodic_tasks(),
        )

    async def stop(self):
        """Gracefully shut down."""
        logger.info("Shutting down...")
        self._running = False
        if self.feed:
            await self.feed.stop()
        if self.mt5_feed:
            await self.mt5_feed.stop()
        if self.ib_feed:
            await self.ib_feed.stop()
        await self.db.close()
        logger.info("System stopped.")

    def _download_mt5_history_sync(self):
        """
        Download historical 1-minute bars from MT5 and inject into candle builders.
        Runs synchronously before the event loop starts so candles are always available.
        """
        if not self.mt5_feed or not self.mt5_feed._mt5:
            logger.warning("No MT5 connection — skipping history download")
            return

        mt5_mod = self.mt5_feed._mt5   # already-initialized MetaTrader5 module
        from datetime import timedelta

        now = datetime.now(timezone.utc)
        from_date = now - timedelta(days=MT5.download_history_days)

        # Use the feed's RESOLVED symbols (after auto-discovery), not the raw
        # config, so history downloads work for broker-specific names (e.g.
        # BTCUSDm -> BTCUSDc).
        resolved_symbols = self.mt5_feed.symbols

        logger.info(
            f"Downloading M1 bars for {len(resolved_symbols)} instruments "
            f"({MT5.download_history_days}d)..."
        )

        total_added = 0
        for internal, mt5_sym in resolved_symbols.items():
            pipeline = self.pipelines.get(internal)
            if not pipeline:
                continue

            try:
                rates = mt5_mod.copy_rates_range(
                    mt5_sym, mt5_mod.TIMEFRAME_M1, from_date, now
                )

                if rates is None or len(rates) == 0:
                    logger.warning(f"[{internal}] No M1 bars for {mt5_sym}")
                    continue

                # Convert to Candle objects
                candles = []
                for r in rates:
                    candles.append(Candle(
                        timestamp_ms=int(r['time']) * 1000,
                        open=float(r['open']),
                        high=float(r['high']),
                        low=float(r['low']),
                        close=float(r['close']),
                        volume=float(
                            r['real_volume'] if r['real_volume'] > 0
                            else r['tick_volume']
                        ),
                        buy_volume=0.0,
                        sell_volume=0.0,
                        tick_count=int(r['tick_volume']),
                    ))

                added = pipeline.candle_builder.load_historical_candles(candles)
                total_added += added

                # Build one clean profile per UTC day from the downloaded bars,
                # so prior days get real daily profiles (not lumped into today).
                n_profiles = self._build_daily_profiles_sync(
                    internal, pipeline, candles
                )

                logger.info(
                    f"[{internal}] +{added} M1 bars "
                    f"(total {len(pipeline.candle_builder.history)}, "
                    f"{n_profiles} daily profiles)"
                )

            except Exception as e:
                logger.error(
                    f"[{internal}] History download failed: {e}",
                    exc_info=True,
                )

        logger.info(
            f"History download complete: {total_added} total candles loaded"
        )

    async def _download_ib_history(self, days: int):
        """
        Download historical bars from IB Gateway and inject into candle
        builders. Waits for the IB feed to connect and qualify contracts
        before requesting history.
        """
        if not self.ib_feed:
            logger.warning("No IB feed — skipping history download")
            return

        # Wait for ALL configured symbols to be qualified before downloading.
        expected = set(self.ib_feed.symbols.keys())
        for _ in range(30):
            if expected.issubset(self.ib_feed._contracts.keys()):
                break
            await asyncio.sleep(1)

        missing = expected - set(self.ib_feed._contracts.keys())
        if missing:
            logger.warning(
                f"IB feed not fully connected — skipping history download "
                f"(missing: {missing})"
            )
            return

        logger.info(
            f"Downloading IB bars for {len(self.ib_feed._contracts)} "
            f"instruments ({days}d)..."
        )
        await self.ib_feed.download_history(
            self.pipelines, timeframe="1 min", days=days
        )

        # Build one clean profile per UTC day from the downloaded candle history
        # (replaces the feed's lumped "today" profile via date-dedup).
        for internal, pipeline in self.pipelines.items():
            history = pipeline.candle_builder.history
            if history:
                await self._build_daily_profiles(internal, pipeline, history)

    async def _on_tick(self, symbol: str, tick: Tick):
        """Handle incoming tick."""
        pipeline = self.pipelines.get(symbol)
        if not pipeline:
            return

        await pipeline.process_tick(tick)

        # Check TP/SL for any active trade on this instrument
        close_agg = self.aggregator.check_tp_sl(symbol, tick.price)
        if close_agg:
            trade = self.aggregator.get_active_trade(symbol)
            # Determine the exit price from the close reason (TP/SL level)
            exit_price = trade.entry_price
            if "TP hit" in trade.notes:
                exit_price = trade.take_profit
            elif "SL hit" in trade.notes:
                exit_price = trade.stop_loss
            # Log the closed trade to the journal
            await self.db.log_trade(
                instrument=symbol,
                direction=trade.direction.value,
                entry_price=trade.entry_price,
                exit_price=exit_price,
                stop_loss=trade.stop_loss,
                take_profit=trade.take_profit,
                pnl_ticks=trade.pnl_ticks,
                rr_ratio=trade.rr_ratio,
                signals=trade.absorption_signals,
                notes=trade.notes,
                entry_time_ms=trade.entry_time_ms,
            )
            # Notify Telegram
            await self.telegram.send_trade_closed(symbol, trade, exit_price, trade.pnl_ticks)
            # Broadcast to dashboard
            await self.ws_manager.broadcast_signal(symbol, close_agg)
            await self.ws_manager.broadcast_trade_state(symbol, trade)

        # Broadcast to dashboard (throttled by ws_manager)
        await self.ws_manager.broadcast_tick(
            symbol, tick.price, tick.size, tick.side.value
        )

        # Buffer ticks for batch DB insert
        if symbol not in self._tick_buffers:
            self._tick_buffers[symbol] = []
        self._tick_buffers[symbol].append(tick)

        if len(self._tick_buffers[symbol]) >= self._tick_batch_size:
            await self.db.insert_ticks_batch(symbol, self._tick_buffers[symbol])
            self._tick_buffers[symbol] = []

    async def _on_orderbook(self, symbol: str, snapshot: OrderbookSnapshot):
        """Handle orderbook update."""
        pipeline = self.pipelines.get(symbol)
        if not pipeline:
            return

        sweep_signal = await pipeline.process_orderbook(snapshot)

        # Broadcast orderbook to dashboard (throttled)
        await self.ws_manager.broadcast_orderbook(symbol, {
            "best_bid": snapshot.best_bid,
            "best_ask": snapshot.best_ask,
            "spread": snapshot.spread,
            "imbalance": round(snapshot.imbalance_ratio(), 4),
        })

        if sweep_signal:
            await self._handle_signal(symbol, pipeline, sweep_signal)

    async def _on_candle_signals(self, symbol: str, signals: list[Signal]):
        """Called by InstrumentPipeline callback when candle-close produces signals."""
        pipeline = self.pipelines.get(symbol)
        if not pipeline:
            return

        for signal in signals:
            await self._handle_signal(symbol, pipeline, signal)

    async def _on_candle_close_handler(self, symbol: str, candle: Candle):
        """
        Called by InstrumentPipeline when a candle closes.
        Saves the candle to the DB and broadcasts it to the dashboard.
        (Signal processing is already handled inside pipeline._on_candle_close.)
        """
        pipeline = self.pipelines.get(symbol)
        if not pipeline:
            return

        # Store candle
        await self.db.insert_candle(symbol, "1m", candle)

        # Broadcast candle to dashboard
        await self.ws_manager.broadcast_candle(symbol, {
            "time": candle.timestamp_ms / 1000,
            "open": round(candle.open, 6),
            "high": round(candle.high, 6),
            "low": round(candle.low, 6),
            "close": round(candle.close, 6),
            "volume": round(candle.volume, 2),
            "delta": round(candle.delta, 2),
        })

        # Broadcast cumulative delta
        await self.ws_manager.broadcast_delta(symbol, {
            "time": candle.timestamp_ms / 1000,
            "value": round(pipeline.delta_engine.cumulative_delta, 2),
            "bar_delta": round(candle.delta, 2),
        })

    async def _handle_signal(
        self, symbol: str, pipeline: InstrumentPipeline, signal: Signal
    ):
        """Route a signal through the aggregator and send alerts."""
        # Store raw signal
        await self.db.insert_signal(symbol, signal)

        # Get current bias
        bias = pipeline.profile_framing.current_bias

        # Aggregate with context
        agg = self.aggregator.process_signal(
            instrument=symbol,
            signal=signal,
            bias=bias,
            current_price=pipeline.current_price,
            recent_candles=pipeline.candle_builder.get_recent_candles(5),
        )

        if agg:
            trade = self.aggregator.get_active_trade(symbol)
            # Automatically execute the trade action in MT5 (if enabled).
            await self._maybe_auto_execute(symbol, agg, trade)

            await self.telegram.send_signal_alert(symbol, agg, bias, trade)

            # Broadcast signal + trade state to dashboard
            await self.ws_manager.broadcast_signal(symbol, agg)
            if trade:
                await self.ws_manager.broadcast_trade_state(symbol, trade)
        else:
            # Not alerted — surface the reason (logged by the aggregator too).
            reason = self.aggregator.get_last_decision(symbol)
            logger.info(
                f"[{symbol}] Signal {signal.signal_type.value} "
                f"{signal.direction.value} NOT alerted: {reason}"
            )

    async def _maybe_auto_execute(
        self, symbol: str, agg, trade
    ):
        """
        If MT5 auto-trading is enabled, mirror the aggregated action into the
        broker: enter/close, or move the SL (break-even / trailing). Outcomes
        are logged and broadcast to the dashboard.
        """
        if self.executor is None or not self.executor.enabled:
            return

        action = agg.action
        result = None
        action_label = action

        try:
            if action == "enter" and trade is not None:
                result = self.executor.execute_market_order(
                    symbol,
                    agg.direction,
                    sl=agg.suggested_sl,
                    tp=agg.suggested_tp,
                    comment=f"orderflow {agg.direction.value}",
                )
                action_label = "ENTRY"
            elif action in ("break_even", "trail") and trade is not None:
                # Move the SL to the updated level (BE or trailing stop).
                result = self.executor.modify_stop_loss(
                    symbol, trade.stop_loss,
                    comment=f"orderflow SL {trade.stop_loss:.2f}",
                )
                action_label = "SL UPDATE"
            elif action == "exit":
                result = self.executor.close_position(
                    symbol, comment="orderflow exit"
                )
                action_label = "EXIT"
        except Exception as exc:
            logger.error(f"[{symbol}] Auto-execute failed: {exc}", exc_info=True)
            result = None

        if result is None:
            logger.info(f"[{symbol}] Auto-execute skipped for action={action}")
            return

        logger.info(
            f"[{symbol}] Auto {action_label}: {result.status.value} — {result.message}"
        )
        # Broadcast the execution outcome to the dashboard as a signal note.
        await self.ws_manager.broadcast(
            "execution",
            {
                "symbol": symbol,
                "action": action_label,
                "status": result.status.value,
                "message": result.message,
                "ticket": result.deal_ticket,
                "price": result.filled_price,
            },
        )

    async def _periodic_tasks(self):
        """Run periodic tasks: daily VP refresh, bias update, stats logging."""
        profile_interval = 300        # Refresh today's UTC-day VP every 5 min (upserts one row/day)
        stats_interval = 15           # Broadcast stats every 15 sec
        last_profile = 0
        last_stats = 0

        while self._running:
            await asyncio.sleep(10)
            now = asyncio.get_event_loop().time()

            # Flush remaining tick buffers
            for symbol in list(self._tick_buffers.keys()):
                if self._tick_buffers[symbol]:
                    await self.db.insert_ticks_batch(
                        symbol, self._tick_buffers[symbol]
                    )
                    self._tick_buffers[symbol] = []

            # Periodic VP rebuild
            if now - last_profile > profile_interval:
                last_profile = now
                for symbol, pipeline in self.pipelines.items():
                    await self._rebuild_volume_profile(symbol, pipeline)

            # Stats logging
            if now - last_stats > stats_interval:
                last_stats = now
                all_stats = []
                for symbol, pipeline in self.pipelines.items():
                    stats = pipeline.stats
                    logger.info(
                        f"[{symbol}] price={stats['price']:.2f} "
                        f"ticks={stats['ticks']} candles={stats['candles']} "
                        f"cum_delta={stats['cum_delta']:.1f}"
                    )
                    all_stats.append(stats)

                # Broadcast system-wide stats
                await self.ws_manager.broadcast_stats({
                    "data_source": self.data_source.value,
                    "ws_clients": self.ws_manager.client_count,
                    "instruments": all_stats,
                })

                # Broadcast per-symbol stats so dashboard updates per tab
                for symbol, pipeline in self.pipelines.items():
                    await self.ws_manager.broadcast(
                        "stats", pipeline.stats, symbol=symbol
                    )

    async def _rebuild_volume_profile(
        self, symbol: str, pipeline: InstrumentPipeline
    ):
        """
        Build the current session-aligned daily volume profile.

        The session window is taken from the instrument's VolumeProfileConfig.
        - Gold (XAUUSD): London+NY session (~07:00–17:00 UTC) — primary liquidity
        - BTC (BTCUSD): full day (0:00–24:00 UTC)

        Persists one clean row per day and updates bias/levels.
        """
        now = datetime.now(timezone.utc)
        now_ms = int(now.timestamp() * 1000)
        session = pipeline.vp_engine.config.session
        start_ms, end_ms = _session_start_end_ms(now, session)

        candles = await self.db.get_candles(symbol, "1m", start_ms, end_ms)
        if len(candles) < 10:
            return

        # Label the profile by the day the session window starts on.
        session_date = datetime.fromtimestamp(start_ms / 1000, tz=timezone.utc)
        today = session_date.strftime("%Y-%m-%d")
        vp = pipeline.vp_engine.compute_from_candles(candles, session_date=today)

        if vp.total_volume > 0:
            pipeline.profile_framing.add_profile(vp)
            await self.db.insert_volume_profile(symbol, vp)
            bias = pipeline.profile_framing.analyze(pipeline.current_price)
            logger.info(
                f"[{symbol}] Daily VP [{today}]: POC={vp.poc:.2f} "
                f"VAH={vp.vah:.2f} VAL={vp.val:.2f} "
                f"Shape={vp.shape} | Bias={bias.direction.value}"
            )

            # Broadcast VP + bias to dashboard
            await self.ws_manager.broadcast_volume_profile(symbol, {
                "poc": vp.poc,
                "vah": vp.vah,
                "val": vp.val,
                "shape": vp.shape,
                "total_volume": vp.total_volume,
                "lvn_levels": vp.lvn_levels,
                "volume_at_price": {
                    str(p): v for p, v in sorted(vp.volume_at_price.items())
                },
            })
            await self.ws_manager.broadcast_bias(symbol, bias)

            # Auto-watch qualified levels
            for level in bias.qualified_levels:
                if level.strength >= 50:
                    self.aggregator.set_watching(
                        symbol, level, level.direction
                    )

    async def _build_daily_profiles(self, symbol: str, pipeline, candles):
        """Build ONE clean volume profile per UTC day from downloaded candles.

        Groups candles by their UTC calendar date, applies the instrument's
        session window (e.g. gold = London+NY 07:00–17:00 UTC), and computes a
        profile for each day. Each daily profile is added to the framing engine
        (dedup by date) and persisted to the DB (upsert — one row per day).

        Returns the number of daily profiles built.
        """
        from collections import defaultdict

        if not candles:
            return 0

        session = pipeline.vp_engine.config.session
        by_day: dict[str, list] = defaultdict(list)
        for c in candles:
            day = datetime.fromtimestamp(c.timestamp_ms / 1000, tz=timezone.utc)
            by_day[day.strftime("%Y-%m-%d")].append(c)

        built = 0
        for day, day_candles in by_day.items():
            day_candles = _filter_candles_by_session(day_candles, session)
            if len(day_candles) < 10:
                continue
            vp = pipeline.vp_engine.compute_from_candles(
                day_candles, session_date=day
            )
            if vp.total_volume <= 0:
                continue
            pipeline.profile_framing.add_profile(vp)
            await self.db.insert_volume_profile(symbol, vp)
            built += 1
            logger.info(
                f"[{symbol}] Built historical daily profile [{day}]: "
                f"POC={vp.poc:.2f} VAH={vp.vah:.2f} VAL={vp.val:.2f} "
                f"Shape={vp.shape} ({len(day_candles)} bars)"
            )
        return built

    def _build_daily_profiles_sync(self, symbol: str, pipeline, candles):
        """Sync variant for the MT5 warmup (runs before the event loop).

        Adds per-day profiles (session-filtered) to the framing engine
        immediately and schedules the DB persistence as a background task (the
        loop is running inside start()). Returns the number of profiles built.
        """
        from collections import defaultdict

        if not candles:
            return 0

        session = pipeline.vp_engine.config.session
        by_day: dict[str, list] = defaultdict(list)
        for c in candles:
            day = datetime.fromtimestamp(c.timestamp_ms / 1000, tz=timezone.utc)
            by_day[day.strftime("%Y-%m-%d")].append(c)

        built = 0
        for day, day_candles in by_day.items():
            day_candles = _filter_candles_by_session(day_candles, session)
            if len(day_candles) < 10:
                continue
            vp = pipeline.vp_engine.compute_from_candles(
                day_candles, session_date=day
            )
            if vp.total_volume <= 0:
                continue
            pipeline.profile_framing.add_profile(vp)
            # Persist in the background (async DB write).
            asyncio.create_task(self.db.insert_volume_profile(symbol, vp))
            built += 1
            logger.info(
                f"[{symbol}] Built historical daily profile [{day}]: "
                f"POC={vp.poc:.2f} VAH={vp.vah:.2f} VAL={vp.val:.2f} "
                f"Shape={vp.shape} ({len(day_candles)} bars)"
            )
        return built


def setup_logging():
    """Configure logging for the system."""
    logging.basicConfig(
        level=getattr(logging, LOG_LEVEL, logging.INFO),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler("orderflow_system.log", encoding="utf-8"),
        ],
    )


def main():
    """Entry point."""
    setup_logging()

    logger.info(f"Data source: {DATA_SOURCE.value.upper()}")
    if DATA_SOURCE in (DataSource.MT5, DataSource.BOTH):
        logger.info(f"MT5 symbols: {MT5.symbols}")
        logger.info(
            "Make sure MetaTrader 5 is running and logged into your broker account."
        )
        if sys.platform.startswith("linux"):
            logger.info(
                "Linux detected: using mt5linux RPC bridge -> Wine-hosted MT5 "
                f"({MT5.bridge_host}:{MT5.bridge_port}). Ensure mt5server.exe "
                "is running inside the Wine prefix."
            )
    if DATA_SOURCE in (DataSource.IB, DataSource.BOTH):
        logger.info(f"IB symbols: {IB.symbols}")
        logger.info(
            "Make sure IB Gateway / TWS is running with API enabled "
            f"at {IB.host}:{IB.port}."
        )

    system = OrderflowSystem(data_source=DATA_SOURCE)

    # Handle Ctrl+C gracefully
    loop = asyncio.new_event_loop()

    def shutdown_handler():
        logger.info("Received shutdown signal...")
        loop.create_task(system.stop())

    if sys.platform != "win32":
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, shutdown_handler)

    try:
        loop.run_until_complete(system.start())
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
        loop.run_until_complete(system.stop())
    finally:
        loop.close()


if __name__ == "__main__":
    main()
