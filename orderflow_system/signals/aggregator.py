"""
Signal Aggregator & State Machine
Combines all pattern signals with volume profile context into actionable trade alerts.

Implements Fabio's execution model as a state machine:
  WATCHING → ABSORPTION_DETECTED → POSITION_OPEN → BREAK_EVEN → TRAILING → CLOSED

Signal weighting:
  - Absorption: 30% (primary entry)
  - Delta/Divergence: 25% (confirmation)
  - Volume Profile context: 25% (level qualification)
  - Initiative/Sweep: 20% (BE trigger / trail)
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Optional

from orderflow_system.data.models import (
    Signal, SignalType, Side, TradeState, TradePhase, Candle,
)
from orderflow_system.signals.profile_framing import DailyBias, QualifiedLevel, LevelType
from orderflow_system.config.settings import BiasDirection

logger = logging.getLogger(__name__)


@dataclass
class AggregatedSignal:
    """Weighted combination of multiple signals at a qualified level."""
    timestamp_ms: int
    direction: Side
    composite_score: float = 0.0       # 0-100
    qualified_level: Optional[QualifiedLevel] = None
    signals: list[Signal] = field(default_factory=list)
    action: str = ""                    # 'enter', 'break_even', 'trail', 'exit', 'alert_only'
    suggested_sl: float = 0.0
    suggested_tp: float = 0.0
    notes: str = ""


class SignalAggregator:
    """
    Combines pattern signals with profile context and manages the trade state machine.

    Flow:
    1. Profile framing qualifies levels and sets daily bias
    2. When price reaches a qualified level, enter WATCHING state
    3. Absorption at the level → ENTRY signal (composite score must pass threshold)
    4. Initiative auction after entry → BREAK EVEN trigger
    5. Subsequent initiative prints → TRAIL stop
    6. Exhaustion or divergence → EXIT / reduce
    """

    def __init__(
        self,
        min_composite_score: float = 60.0,
        signal_cooldown_seconds: float = 60.0,
        price_proximity_pct: float = 0.002,  # 0.2% proximity to qualified level
        min_tp_distance: float = 10.0,       # Min distance from entry to TP (price units)
        min_rr: float = 0.5,                 # Min reward:risk ratio (natural, not forced)
        min_sl_distance: float = 0.0,        # Min distance from entry to SL (price units
                                             # — e.g. 20.0 for BTCUSD to clear the spread)
    ):
        self.min_composite_score = min_composite_score
        self.signal_cooldown_seconds = signal_cooldown_seconds
        self.price_proximity_pct = price_proximity_pct
        self.min_tp_distance = min_tp_distance
        self.min_rr = min_rr
        self.min_sl_distance = min_sl_distance
        self._min_sl_distance: dict[str, float] = {}  # per-instrument override
        self._active_trades: dict[str, TradeState] = {}  # instrument → trade
        self._watched_levels: dict[str, list[QualifiedLevel]] = {}  # instrument → watched levels
        self._last_signal_time: dict[str, int] = {}      # instrument → timestamp_ms
        self._signal_history: list[AggregatedSignal] = []
        # instrument → last decision reason (accepted OR rejected-with-reason)
        self._last_decision: dict[str, str] = {}

    def _note_decision(self, instrument: str, reason: str):
        """Record the outcome of processing the last signal (for logging)."""
        self._last_decision[instrument] = reason

    def set_min_sl_distance(self, instrument: str, distance: float):
        """Override the minimum SL distance (price units) for an instrument."""
        self._min_sl_distance[instrument] = distance

    def _sl_min_for(self, instrument: str) -> float:
        """Minimum SL distance for an instrument (per-instrument or global)."""
        return self._min_sl_distance.get(instrument, self.min_sl_distance)

    def get_last_decision(self, instrument: str) -> str:
        """Return the reason the last signal was/wasn't acted upon."""
        return self._last_decision.get(instrument, "no signal processed")

    def process_signal(
        self,
        instrument: str,
        signal: Signal,
        bias: Optional[DailyBias],
        current_price: float,
        recent_candles: list[Candle],
    ) -> Optional[AggregatedSignal]:
        """
        Process a new pattern signal against the current bias and trade state.
        Returns an aggregated signal if action is needed.
        """
        now_ms = int(time.time() * 1000)

        # Cooldown check
        last_ts = self._last_signal_time.get(instrument, 0)
        if now_ms - last_ts < self.signal_cooldown_seconds * 1000:
            reason = (
                f"cooldown ({(now_ms - last_ts) / 1000:.0f}s < "
                f"{self.signal_cooldown_seconds}s since last action)"
            )
            self._note_decision(instrument, reason)
            logger.info(f"[{instrument}] Signal NOT alerted: {reason}")
            return None

        active_trade = self._active_trades.get(instrument)
        watched = self._watched_levels.get(instrument, [])

        # ── State machine routing ──

        if active_trade is None or active_trade.phase == TradePhase.CLOSED:
            # No active trade — check for new entry
            return self._check_new_entry(
                instrument, signal, bias, current_price, now_ms
            )

        elif active_trade.phase == TradePhase.WATCHING:
            # Watching a qualified level — look for absorption or sweep
            if signal.signal_type == SignalType.ABSORPTION:
                return self._handle_absorption_at_level(
                    instrument, signal, bias, active_trade, current_price, now_ms
                )
            elif signal.signal_type == SignalType.SWEEP:
                # Sweep at watched level — generate alert but don't enter
                return self._handle_sweep_at_level(
                    instrument, signal, bias, active_trade, current_price, now_ms
                )
            else:
                reason = (
                    f"WATCHING but signal type {signal.signal_type.value} "
                    f"is not absorption/sweep"
                )
                self._note_decision(instrument, reason)
                logger.info(f"[{instrument}] Signal NOT alerted: {reason}")
                return None

        elif active_trade.phase == TradePhase.ABSORPTION_DETECTED:
            # Absorption already detected but not yet a position (e.g. a prior
            # entry was rejected). Re-evaluate a fresh absorption signal.
            if signal.signal_type == SignalType.ABSORPTION:
                return self._handle_absorption_at_level(
                    instrument, signal, bias, active_trade, current_price, now_ms
                )
            else:
                reason = (
                    f"ABSORPTION_DETECTED but signal type "
                    f"{signal.signal_type.value} is not absorption"
                )
                self._note_decision(instrument, reason)
                logger.info(f"[{instrument}] Signal NOT alerted: {reason}")
                return None

        elif active_trade.phase == TradePhase.POSITION_OPEN:
            # Position open, waiting for BE trigger
            if signal.signal_type == SignalType.INITIATIVE:
                return self._handle_initiative_for_be(
                    instrument, signal, active_trade, now_ms
                )
            elif signal.signal_type in (SignalType.EXHAUSTION, SignalType.DIVERGENCE):
                return self._handle_exit_warning(
                    instrument, signal, active_trade, now_ms
                )
            else:
                reason = (
                    f"POSITION_OPEN but signal type {signal.signal_type.value} "
                    f"is not initiative/exit"
                )
                self._note_decision(instrument, reason)
                logger.info(f"[{instrument}] Signal NOT alerted: {reason}")
                return None

        elif active_trade.phase in (TradePhase.BREAK_EVEN, TradePhase.TRAILING):
            # Trailing — update trail or detect exit
            if signal.signal_type == SignalType.INITIATIVE:
                return self._handle_initiative_for_trail(
                    instrument, signal, active_trade, recent_candles, now_ms
                )
            elif signal.signal_type in (SignalType.EXHAUSTION, SignalType.DIVERGENCE):
                return self._handle_exit_warning(
                    instrument, signal, active_trade, now_ms
                )
            else:
                reason = (
                    f"{active_trade.phase.value} but signal type "
                    f"{signal.signal_type.value} is not initiative/exit"
                )
                self._note_decision(instrument, reason)
                logger.info(f"[{instrument}] Signal NOT alerted: {reason}")
                return None

        reason = f"unhandled state (phase={active_trade.phase.value if active_trade else 'NONE'})"
        self._note_decision(instrument, reason)
        logger.info(f"[{instrument}] Signal NOT alerted: {reason}")
        return None

    def set_watching(
        self, instrument: str, level: QualifiedLevel, direction: Side
    ):
        """Begin watching a qualified level for entry signals."""
        # Track multiple watched levels per instrument (don't overwrite)
        if instrument not in self._watched_levels:
            self._watched_levels[instrument] = []

        # Avoid duplicate levels (same price within 0.01%)
        for existing in self._watched_levels[instrument]:
            if abs(existing.price - level.price) / max(level.price, 1) < 0.0001:
                return  # Already watching this level

        self._watched_levels[instrument].append(level)

        # Only create WATCHING trade if no active trade yet
        active = self._active_trades.get(instrument)
        if active is None or active.phase == TradePhase.CLOSED:
            trade = TradeState(
                instrument=instrument,
                direction=direction,
                phase=TradePhase.WATCHING,
                qualified_level=level.price,
            )
            self._active_trades[instrument] = trade
            logger.info(
                f"[{instrument}] WATCHING {level.level_type.value} "
                f"@ {level.price:.2f} for {'LONG' if direction == Side.BUY else 'SHORT'}"
            )

    def _check_new_entry(
        self,
        instrument: str,
        signal: Signal,
        bias: Optional[DailyBias],
        current_price: float,
        now_ms: int,
    ) -> Optional[AggregatedSignal]:
        """Check if a new signal qualifies for entry at a profile level."""
        # Only ONE active entry per instrument: never open a second position
        # while one is already open.
        if self.has_open_position(instrument):
            reason = (
                "already have an open position in this instrument "
                "(no second entry)"
            )
            self._note_decision(instrument, reason)
            logger.info(f"[{instrument}] Entry NOT alerted: {reason}")
            return None

        if signal.signal_type != SignalType.ABSORPTION:
            reason = f"signal type {signal.signal_type.value} is not ABSORPTION"
            self._note_decision(instrument, reason)
            logger.info(f"[{instrument}] Entry NOT alerted: {reason}")
            return None  # Only absorption triggers new entries

        if bias is None or not bias.qualified_levels:
            reason = "no qualified levels from bias (bias/session may be incomplete)"
            self._note_decision(instrument, reason)
            logger.info(f"[{instrument}] Entry NOT alerted: {reason}")
            return None

        # Find nearest qualified level to current price
        nearest = None
        min_dist = float("inf")

        for level in bias.qualified_levels:
            dist = abs(current_price - level.price) / max(current_price, 1.0)
            if dist < min_dist and dist < self.price_proximity_pct:
                min_dist = dist
                nearest = level

        if nearest is None:
            reason = (
                f"price {current_price:.2f} not within {self.price_proximity_pct:.4f} "
                f"of any qualified level"
            )
            self._note_decision(instrument, reason)
            logger.info(f"[{instrument}] Entry NOT alerted: {reason}")
            return None  # Not near any qualified level

        # Check direction alignment
        if nearest.direction != signal.direction:
            reason = (
                f"nearest level {nearest.level_type.value}@{nearest.price:.2f} "
                f"is {nearest.direction.value} but signal is {signal.direction.value}"
            )
            self._note_decision(instrument, reason)
            logger.info(f"[{instrument}] Entry NOT alerted: {reason}")
            return None

        # Compute composite score
        score = self._compute_composite_score(signal, nearest, bias)

        if score < self.min_composite_score:
            reason = (
                f"composite score {score:.0f} < {self.min_composite_score} "
                f"(sig_str={signal.strength:.0f}, level_str={nearest.strength:.0f}, "
                f"bias={bias.direction.value}, bias_conf={bias.confidence:.0f})"
            )
            self._note_decision(instrument, reason)
            logger.info(f"[{instrument}] Entry NOT alerted: {reason}")
            return None

        # Create trade state
        trade = TradeState(
            instrument=instrument,
            direction=signal.direction,
            qualified_level=nearest.price,
        )
        trade.advance_to_absorption(signal)

        # Compute SL/TP and advance to position
        sl_min = self._sl_min_for(instrument)
        sl, tp = self._compute_sl_tp(
            signal.direction, nearest, bias, current_price, min_sl_distance=sl_min
        )

        # Reject entries where TP is too close to entry (noise / bad R:R).
        if signal.direction == Side.BUY:
            tp_dist = tp - current_price
        else:
            tp_dist = current_price - tp
        if tp_dist < self.min_tp_distance:
            reason = (
                f"TP distance {tp_dist:.2f} < {self.min_tp_distance} "
                f"(TP {tp:.2f} too close to entry {current_price:.2f})"
            )
            self._note_decision(instrument, reason)
            logger.info(f"[{instrument}] Entry NOT alerted: {reason}")
            return None

        # Reject entries where the SL is on the WRONG SIDE of entry. A long's
        # stop must be below entry (and a short's above); otherwise the stop is
        # meaningless and a later "SL hit" would log a false profit.
        if not self._sl_is_valid(signal.direction, current_price, sl):
            reason = (
                f"SL {sl:.2f} on wrong side of entry {current_price:.2f} "
                f"for {signal.direction.value.upper()} (stop must be "
                f"{'below' if signal.direction == Side.BUY else 'above'} entry)"
            )
            self._note_decision(instrument, reason)
            logger.info(f"[{instrument}] Entry NOT alerted: {reason}")
            return None

        # Reject entries where the SL is too close to entry (doesn't clear the
        # spread / would be stopped by normal noise).
        sl_dist = self._sl_distance(signal.direction, current_price, sl)
        if sl_dist < sl_min:
            reason = (
                f"SL distance {sl_dist:.2f} < {sl_min} "
                f"(SL {sl:.2f} too tight vs entry {current_price:.2f})"
            )
            self._note_decision(instrument, reason)
            logger.info(f"[{instrument}] Entry NOT alerted: {reason}")
            return None

        # Reject entries where the natural R:R is below the minimum.
        risk = abs(current_price - sl)
        rr = (tp_dist / risk) if risk > 0 else 0.0
        if rr < self.min_rr:
            reason = (
                f"R:R {rr:.2f} < {self.min_rr} "
                f"(risk {risk:.2f}, reward {tp_dist:.2f})"
            )
            self._note_decision(instrument, reason)
            logger.info(f"[{instrument}] Entry NOT alerted: {reason}")
            return None

        trade.advance_to_position(current_price, sl, tp)
        self._active_trades[instrument] = trade

        agg = AggregatedSignal(
            timestamp_ms=now_ms,
            direction=signal.direction,
            composite_score=score,
            qualified_level=nearest,
            signals=[signal],
            action="enter",
            suggested_sl=sl,
            suggested_tp=tp,
            notes=(
                f"ENTRY SIGNAL: Absorption at {nearest.level_type.value} "
                f"({nearest.price:.2f}). Score: {score:.0f}. "
                f"SL: {sl:.2f}, TP: {tp:.2f}"
            ),
        )
        self._last_signal_time[instrument] = now_ms
        self._signal_history.append(agg)
        self._note_decision(
            instrument, f"ENTRY alerted: score {score:.0f}, SL {sl:.2f}, TP {tp:.2f}"
        )
        return agg

    def _handle_absorption_at_level(
        self,
        instrument: str,
        signal: Signal,
        bias: Optional[DailyBias],
        trade: TradeState,
        current_price: float,
        now_ms: int,
    ) -> Optional[AggregatedSignal]:
        """Handle absorption signal while watching a level."""
        # Only ONE active entry per instrument: never open a second position
        # while one is already open.
        if self.has_open_position(instrument):
            reason = (
                "already have an open position in this instrument "
                "(no second entry)"
            )
            self._note_decision(instrument, reason)
            logger.info(f"[{instrument}] Entry NOT alerted: {reason}")
            return None

        # Find nearest watched level matching signal direction
        watched = self._watched_levels.get(instrument, [])
        nearest_level = None
        min_dist = float("inf")
        for wl in watched:
            if wl.direction != signal.direction:
                continue
            dist = abs(current_price - wl.price) / max(current_price, 1.0)
            if dist < min_dist and dist < self.price_proximity_pct:
                min_dist = dist
                nearest_level = wl

        if nearest_level is None:
            # Fall back to the nearest QUALIFIED level matching the SIGNAL
            # direction (not the stale `trade.direction`). The trade direction
            # only reflects whichever level was watched FIRST (often a LONG VAL),
            # so a SHORT absorption near its own qualified level must not be
            # rejected just because the active WATCHING trade was set LONG.
            nearest_level = self._find_qualified_level_by_direction(
                bias, signal.direction, current_price
            )
            if nearest_level is None:
                reason = (
                    f"no qualified level near price matching "
                    f"signal dir {signal.direction.value}"
                )
                self._note_decision(instrument, reason)
                logger.info(f"[{instrument}] Entry NOT alerted: {reason}")
                return None

        # Compute composite score BEFORE advancing trade state, so the
        # dashboard and Telegram alert use the same criteria.
        score = self._compute_composite_score(signal, nearest_level, bias)

        if score < self.min_composite_score:
            reason = (
                f"composite score {score:.0f} < {self.min_composite_score} "
                f"(sig_str={signal.strength:.0f}, "
                f"level_str={nearest_level.strength if nearest_level else 0:.0f}, "
                f"bias={bias.direction.value if bias else 'NONE'})"
            )
            self._note_decision(instrument, reason)
            logger.info(f"[{instrument}] Entry NOT alerted: {reason}")
            return None

        # The WATCHING trade's direction was set by set_watching (from the level).
        # The absorption signal may be in the OPPOSITE direction (e.g. watching a
        # LONG VAL level but a SHORT absorption signal confirms at VAH). The trade
        # direction must follow the CONFIRMED signal, not the watched level.
        trade.direction = signal.direction

        sl_min = self._sl_min_for(instrument)
        sl, tp = self._compute_sl_tp(
            signal.direction, nearest_level, bias, current_price,
            min_sl_distance=sl_min,
        )

        # Reject entries where TP is too close to entry (noise / bad R:R).
        if signal.direction == Side.BUY:
            tp_dist = tp - current_price
        else:
            tp_dist = current_price - tp
        if tp_dist < self.min_tp_distance:
            reason = (
                f"TP distance {tp_dist:.2f} < {self.min_tp_distance} "
                f"(TP {tp:.2f} too close to entry {current_price:.2f})"
            )
            self._note_decision(instrument, reason)
            logger.info(f"[{instrument}] Entry NOT alerted: {reason}")
            return None

        # Reject entries where the SL is on the WRONG SIDE of entry. A long's
        # stop must be below entry (and a short's above); otherwise the stop is
        # meaningless and a later "SL hit" would log a false profit.
        if not self._sl_is_valid(signal.direction, current_price, sl):
            reason = (
                f"SL {sl:.2f} on wrong side of entry {current_price:.2f} "
                f"for {signal.direction.value.upper()} (stop must be "
                f"{'below' if signal.direction == Side.BUY else 'above'} entry)"
            )
            self._note_decision(instrument, reason)
            logger.info(f"[{instrument}] Entry NOT alerted: {reason}")
            return None

        # Reject entries where the SL is too close to entry (doesn't clear the
        # spread / would be stopped by normal noise).
        sl_dist = self._sl_distance(signal.direction, current_price, sl)
        if sl_dist < sl_min:
            reason = (
                f"SL distance {sl_dist:.2f} < {sl_min} "
                f"(SL {sl:.2f} too tight vs entry {current_price:.2f})"
            )
            self._note_decision(instrument, reason)
            logger.info(f"[{instrument}] Entry NOT alerted: {reason}")
            return None

        # Reject entries where the natural R:R is below the minimum.
        risk = abs(current_price - sl)
        rr = (tp_dist / risk) if risk > 0 else 0.0
        if rr < self.min_rr:
            reason = (
                f"R:R {rr:.2f} < {self.min_rr} "
                f"(risk {risk:.2f}, reward {tp_dist:.2f})"
            )
            self._note_decision(instrument, reason)
            logger.info(f"[{instrument}] Entry NOT alerted: {reason}")
            return None

        # Only advance to absorption once all filters pass, so the trade is
        # never left stuck in ABSORPTION_DETECTED after a rejection.
        trade.advance_to_absorption(signal)

        # Advance to position with SL/TP
        trade.advance_to_position(current_price, sl, tp)

        agg = AggregatedSignal(
            timestamp_ms=now_ms,
            direction=signal.direction,
            composite_score=score,
            qualified_level=nearest_level,
            signals=[signal],
            action="enter",
            suggested_sl=sl,
            suggested_tp=tp,
            notes=(
                f"ENTRY: Absorption confirmed at watched level "
                f"{nearest_level.price:.2f}. Attempts: {len(trade.absorption_signals)}"
            ),
        )
        self._last_signal_time[instrument] = now_ms
        self._signal_history.append(agg)
        return agg

    def _handle_initiative_for_be(
        self,
        instrument: str,
        signal: Signal,
        trade: TradeState,
        now_ms: int,
    ) -> Optional[AggregatedSignal]:
        """Initiative after entry → move to break even."""
        if signal.direction != trade.direction:
            return None

        trade.advance_to_break_even(signal)

        agg = AggregatedSignal(
            timestamp_ms=now_ms,
            direction=trade.direction,
            composite_score=signal.strength,
            signals=[signal],
            action="break_even",
            notes=(
                f"BREAK EVEN: Initiative auction confirmed. "
                f"Move SL to entry {trade.break_even_price:.2f}"
            ),
        )
        self._last_signal_time[instrument] = now_ms
        self._signal_history.append(agg)
        return agg

    def _handle_initiative_for_trail(
        self,
        instrument: str,
        signal: Signal,
        trade: TradeState,
        recent_candles: list[Candle],
        now_ms: int,
    ) -> Optional[AggregatedSignal]:
        """Subsequent initiative prints → trail stop."""
        if signal.direction != trade.direction:
            return None

        # Trail to the low of the initiative candle (for longs) or high (for shorts)
        if recent_candles:
            last_candle = recent_candles[-1]
            if trade.direction == Side.BUY:
                new_trail = last_candle.low
            else:
                new_trail = last_candle.high
        else:
            new_trail = signal.price_level

        trade.update_trail(new_trail, signal)

        agg = AggregatedSignal(
            timestamp_ms=now_ms,
            direction=trade.direction,
            composite_score=signal.strength,
            signals=[signal],
            action="trail",
            notes=(
                f"TRAIL: New initiative print. "
                f"Move SL to {trade.trail_stop:.2f}"
            ),
        )
        self._last_signal_time[instrument] = now_ms
        self._signal_history.append(agg)
        return agg

    def _handle_exit_warning(
        self,
        instrument: str,
        signal: Signal,
        trade: TradeState,
        now_ms: int,
    ) -> Optional[AggregatedSignal]:
        """Exhaustion or divergence → warning to exit/tighten."""
        # Only warn if signal is AGAINST current trade direction
        if signal.direction == trade.direction:
            return None  # Same direction exhaustion/divergence = less relevant

        action = "exit_warning"
        if signal.strength >= 70:
            action = "exit"
            # Auto-close trade on strong exit signal
            trade.close_trade(signal.price_level, f"{signal.signal_type.value} exit (strength {signal.strength:.0f})")

        agg = AggregatedSignal(
            timestamp_ms=now_ms,
            direction=signal.direction,
            composite_score=signal.strength,
            signals=[signal],
            action=action,
            notes=(
                f"{'EXIT' if action == 'exit' else 'WARNING'}: "
                f"{signal.signal_type.value} detected against position. "
                f"Strength: {signal.strength:.0f}"
            ),
        )
        self._last_signal_time[instrument] = now_ms
        self._signal_history.append(agg)
        return agg

    def _handle_sweep_at_level(
        self,
        instrument: str,
        signal: Signal,
        bias: Optional[DailyBias],
        trade: TradeState,
        current_price: float,
        now_ms: int,
    ) -> Optional[AggregatedSignal]:
        """Handle sweep signal while watching a level — alert only, adds context."""
        agg = AggregatedSignal(
            timestamp_ms=now_ms,
            direction=signal.direction,
            composite_score=signal.strength,
            signals=[signal],
            action="alert_only",
            notes=(
                f"SWEEP detected near watched level @ {trade.qualified_level:.2f}. "
                f"Strength: {signal.strength:.0f} — watch for absorption follow-up"
            ),
        )
        self._last_signal_time[instrument] = now_ms
        self._signal_history.append(agg)
        return agg

    def _compute_composite_score(
        self,
        signal: Signal,
        level: Optional[QualifiedLevel],
        bias: Optional[DailyBias],
    ) -> float:
        """
        Weighted composite score:
          Absorption: 30%
          Delta/Divergence: 25%
          VP context (level strength): 25%
          Initiative/Sweep: 20%
        """
        score = 0.0

        # Signal strength component (30-40% depending on type)
        if signal.signal_type == SignalType.ABSORPTION:
            score += signal.strength * 0.30
        elif signal.signal_type in (SignalType.DIVERGENCE,):
            score += signal.strength * 0.25
        elif signal.signal_type == SignalType.INITIATIVE:
            score += signal.strength * 0.20
        elif signal.signal_type == SignalType.SWEEP:
            score += signal.strength * 0.20
        else:
            score += signal.strength * 0.15

        # Volume profile context (25%)
        if level:
            score += level.strength * 0.25

        # Bias alignment (remaining %)
        if bias:
            bias_aligned = (
                (bias.direction == BiasDirection.LONG and signal.direction == Side.BUY)
                or (bias.direction == BiasDirection.SHORT and signal.direction == Side.SELL)
            )
            if bias_aligned:
                score += bias.confidence * 0.20
            elif bias.direction == BiasDirection.WARNING:
                score -= 10  # Penalty for trading against warning
            else:
                # NEUTRAL bias: no directional alignment, but the level itself
                # already encodes the expected direction. Give a modest baseline
                # so neutral/range-bound markets remain tradeable instead of
                # structurally capping the score below the entry threshold.
                score += 10

        return min(100.0, max(0.0, score))

    def _compute_sl_tp(
        self,
        direction: Side,
        level: Optional[QualifiedLevel],
        bias: Optional[DailyBias],
        current_price: float,
        min_sl_distance: float = 0.0,
    ) -> tuple[float, float]:
        """
        Compute suggested stop loss and take profit.

        The SL is placed from the value area (VAL/VAH) when possible, but it is
        ALWAYS clamped to the correct side of the ENTRY price so it can never be
        inverted (a long's stop below entry, a short's above). This fixes setups
        where price has already moved past the value-area anchor (e.g. a long
        whose fill is below VAL) — the old code produced an inverted stop there.
        """
        if bias is None:
            # Default: 0.3% SL, 0.6% TP
            if direction == Side.BUY:
                sl, tp = current_price * 0.997, current_price * 1.006
            else:
                sl, tp = current_price * 1.003, current_price * 0.994
            return self._clamp_sl(direction, current_price, sl, tp, min_sl_distance)

        if direction == Side.BUY:
            # SL below VAL or absorption zone; TP at VAH.
            sl = bias.val - (bias.vah - bias.val) * 0.1
            tp = bias.vah
        else:
            # SL above VAH; TP at VAL.
            sl = bias.vah + (bias.vah - bias.val) * 0.1
            tp = bias.val

        return self._clamp_sl(direction, current_price, sl, tp, min_sl_distance)

    @staticmethod
    def _clamp_sl(
        direction: Side,
        entry: float,
        sl: float,
        tp: float,
        min_sl_distance: float,
    ) -> tuple[float, float]:
        """Return the natural SL/TP unchanged.

        The SL is NOT widened here. A too-tight SL (closer than
        `min_sl_distance` to entry) is left as-is so the downstream
        `sl_dist < sl_min` check in the entry paths REJECTS the setup.
        An inverted SL (wrong side of entry) is likewise left for the
        `_sl_is_valid` check to reject. This makes the validation strict:
        a setup only alerts if its NATURAL stop already clears the spread
        and sits on the correct side — it is never silently adjusted.
        """
        return sl, tp

    @staticmethod
    def _sl_distance(direction: Side, entry: float, sl: float) -> float:
        """Price-unit distance from entry to the stop loss."""
        return abs(entry - sl)

    @staticmethod
    def _sl_is_valid(direction: Side, entry: float, sl: float) -> bool:
        """A real stop must be on the correct side of entry:
        below entry for a LONG, above entry for a SHORT.

        If the fill/entry price lands past the value-area stop (e.g. a long
        whose entry is BELOW the SL), the stop becomes meaningless and the
        recorded 'exit' would report a false profit. Reject such setups.
        """
        if direction == Side.BUY:
            return sl < entry
        else:
            return sl > entry

    def _fix_invalid_sl(self, trade: TradeState):
        """Move an inverted SL to the correct side of entry.

        Uses the original risk distance (|entry - sl|) so the trade keeps a
        sensible stop, but places it on the correct side. If the distance is
        zero, falls back to the entry price (no risk room).
        """
        dist = abs(trade.entry_price - trade.stop_loss)
        if trade.direction == Side.BUY:
            trade.stop_loss = trade.entry_price - dist
        else:
            trade.stop_loss = trade.entry_price + dist

    def _find_qualified_level(
        self, bias: Optional[DailyBias], price: float
    ) -> Optional[QualifiedLevel]:
        """Find the qualified level closest to a price."""
        if bias is None or not bias.qualified_levels:
            return None
        return min(
            bias.qualified_levels,
            key=lambda lv: abs(lv.price - price),
        )

    def _find_qualified_level_by_direction(
        self,
        bias: Optional[DailyBias],
        direction: Side,
        current_price: float,
    ) -> Optional[QualifiedLevel]:
        """Find the nearest QUALIFIED level (within proximity) whose direction
        matches `direction`. Used so a SHORT absorption can enter at a SHORT
        (e.g. VAH) level even when the active WATCHING trade was set for the
        opposite direction by an earlier level.
        """
        if bias is None or not bias.qualified_levels:
            return None
        best: Optional[QualifiedLevel] = None
        min_dist = float("inf")
        for lv in bias.qualified_levels:
            if lv.direction != direction:
                continue
            dist = abs(current_price - lv.price) / max(current_price, 1.0)
            if dist < min_dist and dist < self.price_proximity_pct:
                min_dist = dist
                best = lv
        return best

    def get_active_trade(self, instrument: str) -> Optional[TradeState]:
        return self._active_trades.get(instrument)

    def has_open_position(self, instrument: str) -> bool:
        """True if there is a live (entryable) position on this instrument.

        Only WATCHING (setting up) and ABSORPTION_DETECTED (pre-entry) phases
        are "not yet open". Once a position is open (POSITION_OPEN /
        BREAK_EVEN / TRAILING) it counts as an active trade → no second entry.
        """
        trade = self._active_trades.get(instrument)
        if trade is None:
            return False
        return trade.phase in (
            TradePhase.POSITION_OPEN,
            TradePhase.BREAK_EVEN,
            TradePhase.TRAILING,
        )

    def close_trade(self, instrument: str, exit_price: float, reason: str = ""):
        trade = self._active_trades.get(instrument)
        if trade:
            trade.close_trade(exit_price, reason)

    def check_tp_sl(
        self, instrument: str, current_price: float
    ) -> Optional[AggregatedSignal]:
        """
        Check the current price against the active trade's TP/SL.
        If either is hit, close the trade and return a close signal.
        Returns None if no active trade or no TP/SL hit.
        """
        trade = self._active_trades.get(instrument)
        if trade is None or trade.phase in (TradePhase.WATCHING, TradePhase.CLOSED):
            return None

        # Only check once a position is actually open (has entry/SL/TP)
        if trade.entry_price <= 0 or trade.stop_loss <= 0 or trade.take_profit <= 0:
            return None

        # Defensive: if the SL ended up on the WRONG side of entry (shouldn't
        # happen now that entries are validated, but protects against legacy
        # or externally-created trades), correct it so it can't fabricate a
        # TP/SL close that reports a false profit.
        if not self._sl_is_valid(trade.direction, trade.entry_price, trade.stop_loss):
            logger.warning(
                f"[{instrument}] Fixing invalid SL {trade.stop_loss:.2f} for "
                f"{trade.direction.value.upper()} entry {trade.entry_price:.2f}"
            )
            self._fix_invalid_sl(trade)

        hit = None
        if trade.direction == Side.BUY:
            if current_price >= trade.take_profit:
                hit = ("TP", trade.take_profit)
            elif current_price <= trade.stop_loss:
                hit = ("SL", trade.stop_loss)
        else:  # SELL
            if current_price <= trade.take_profit:
                hit = ("TP", trade.take_profit)
            elif current_price >= trade.stop_loss:
                hit = ("SL", trade.stop_loss)

        if hit is None:
            return None

        kind, exit_price = hit
        reason = f"{kind} hit @ {exit_price:.2f}"
        trade.close_trade(exit_price, reason)

        agg = AggregatedSignal(
            timestamp_ms=int(time.time() * 1000),
            direction=trade.direction,
            composite_score=100.0,
            signals=[],
            action="exit",
            notes=reason,
        )
        self._signal_history.append(agg)
        logger.info(f"[{instrument}] Trade closed: {reason}")
        return agg

    @property
    def signal_history(self) -> list[AggregatedSignal]:
        return self._signal_history
