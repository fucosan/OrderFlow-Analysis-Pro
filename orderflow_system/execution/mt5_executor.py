"""
Automated execution on MetaTrader 5 via the Wine-hosted MT5 bridge.

This module lets the system place/close trades automatically in the MT5
terminal that the data feed is already connected to (Windows native module or
Linux RPyC bridge). It is designed as a *guarded* auto-trader:

  - Only acts on explicit, validated requests (never invents lot sizes).
  - Never fires if the handle / bridge is unavailable.
  - Tracks the last deal ticket so it can close the position it opened.

Usage:
    handler = MT5ExecutionHandler(mt5_feed, default_volume=0.01)
    ok, msg = handler.execute_market_order("BTCUSDT", Side.BUY, sl=..., tp=...)
    ok, msg = handler.close_position("BTCUSDT")

By default auto-execution is DISABLED. Set `MT5_AUTO_TRADE=1` (or pass
`enabled=True`) to allow the system to place real orders. Keep it disabled
until you've paper-traded / validated the signals.

Per-instrument control:
  - Pass `instruments={"BTCUSDT", ...}` (or set env `MT5_AUTO_TRADE_SYMBOLS`,
    comma-separated, e.g. `MT5_AUTO_TRADE_SYMBOLS=BTCUSDT,XAUUSDT`) to restrict
    auto-trading to only those instruments. Empty allowlist = all instruments.
  - Methods `enable_instrument` / `disable_instrument` / `set_auto_instruments`
    / `is_auto_enabled` let you change the allowlist at runtime.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from enum import Enum
from typing import Optional

from orderflow_system.data.models import Side

logger = logging.getLogger(__name__)

# MT5 trade return codes (result.retcode)
RETCODE_DONE = 10009                 # Request completed successfully
RETCODE_DONE_PARTIAL = 10010         # Partially filled
RETCODE_PRICE_OFF = 10015            # Invalid price


class ExecutionStatus(Enum):
    OK = "ok"
    REJECTED = "rejected"
    DISABLED = "disabled"
    NO_HANDLE = "no_handle"
    ERROR = "error"


@dataclass
class ExecutionResult:
    """Outcome of an execution attempt."""
    status: ExecutionStatus
    message: str = ""
    order_ticket: int = 0
    deal_ticket: int = 0
    filled_price: float = 0.0


def _build_market_request(
    symbol: str,
    side: Side,
    volume: float,
    sl: float = 0.0,
    tp: float = 0.0,
    deal_type: int = 0,           # mt5.ORDER_TYPE_BUY / SELL
    magic: int = 0,
    comment: str = "",
) -> dict:
    """Build a MetaTrader5 market-order request dict."""
    request = {
        "action": 1,             # TRADE_ACTION_DEAL
        "symbol": symbol,
        "volume": float(volume),
        "type": deal_type,
        "type_filling": 2,       # ORDER_FILLING_RETURN (most compatible)
        "deviation": 50,         # max slippage in points (50 = 5 pips)
        "magic": magic,
    }
    if sl:
        request["sl"] = float(sl)
    if tp:
        request["tp"] = float(tp)
    if comment:
        request["comment"] = comment
    return request


class MT5ExecutionHandler:
    """
    Thin, guarded wrapper around the MT5 trade API tied to the live data feed.

    Reuses the feed's already-initialized MT5 handle, so on Linux it executes
    on the SAME Wine-hosted MT5 terminal that provides the tick data — no
    second connection or separate bridge needed.

    Extra safety:
      - `enabled` gate (config MT5_AUTO_TRADE / enabled flag)
      - optional `max_volume` cap
      - `global_cooldown` between automatic orders
    """

    def __init__(
        self,
        mt5_feed,
        default_volume: float = 0.01,
        max_volume: float = 0.0,      # 0 = no cap
        enabled: Optional[bool] = None,
        cooldown_seconds: float = 10.0,
        magic: int = 500100,
        instruments: Optional[set[str]] = None,
    ):
        self.mt5_feed = mt5_feed
        self.default_volume = default_volume
        self.max_volume = max_volume
        self.cooldown_seconds = cooldown_seconds
        self.magic = magic
        self._last_order_time = 0.0
        # Enabled defaults to env MT5_AUTO_TRADE=1; can be overridden explicitly.
        if enabled is None:
            enabled = os.getenv("MT5_AUTO_TRADE", "0") == "1"
        self.enabled = enabled
        self._last_ticket = 0

        # Per-instrument allowlist. Only these internal instrument names
        # (e.g. "BTCUSDT") may be auto-traded. Additive to the global
        # `enabled` gate — if `instruments` is provided (non-empty), an
        # instrument must ALSO be in this set to trade. If empty, auto-trade
        # applies to all instruments.
        # Default patch from env MT5_AUTO_TRADE_SYMBOLS (comma-separated).
        if instruments is None:
            raw = os.getenv("MT5_AUTO_TRADE_SYMBOLS", "")
            instruments = {
                s.strip().upper() for s in raw.split(",") if s.strip()
            }
        self._auto_instruments: set[str] = set(instruments or {})

    # ── per-instrument control ──

    def get_auto_instruments(self) -> set[str]:
        """Return the allowlist of auto-tradable instruments (empty = all)."""
        return set(self._auto_instruments)

    def enable_instrument(self, internal: str):
        """Allow automatic trading on a specific instrument."""
        self._auto_instruments.add(internal.upper())

    def disable_instrument(self, internal: str):
        """Prevent automatic trading on a specific instrument."""
        self._auto_instruments.discard(internal.upper())

    def set_auto_instruments(self, instruments: set[str]):
        """Replace the allowlist (empty set = all instruments)."""
        self._auto_instruments = {s.upper() for s in (instruments or {})}

    def is_auto_enabled(self, internal: str) -> bool:
        """True if auto-trading is allowed for this instrument.

        Requires the global `enabled` gate AND, if an allowlist is configured,
        membership in that allowlist.
        """
        if not self.enabled:
            return False
        if self._auto_instruments:
            return internal.upper() in self._auto_instruments
        return True

    # ── public API ──

    def execute_market_order(
        self,
        internal: str,
        side: Side,
        volume: Optional[float] = None,
        sl: float = 0.0,
        tp: float = 0.0,
        comment: str = "orderflow-auto",
    ) -> ExecutionResult:
        """Place a market order on the internal instrument (e.g. BTCUSDT)."""
        if not self.is_auto_enabled(internal):
            return ExecutionResult(
                ExecutionStatus.DISABLED,
                f"Auto-trade disabled for {internal} "
                "(global MT5_AUTO_TRADE=1 + allowlist required)",
            )

        import time
        now = time.time()
        if now - self._last_order_time < self.cooldown_seconds:
            wait = self.cooldown_seconds - (now - self._last_order_time)
            return ExecutionResult(
                ExecutionStatus.REJECTED,
                f"Execution cooldown ({wait:.0f}s remaining)",
            )

        mt5_sym = self.mt5_feed.get_mt5_symbol(internal)
        if not mt5_sym:
            return ExecutionResult(
                ExecutionStatus.NO_HANDLE, f"No MT5 symbol mapped for {internal}"
            )

        mt5 = getattr(self.mt5_feed, "_mt5", None)
        if mt5 is None:
            return ExecutionResult(
                ExecutionStatus.NO_HANDLE, "MT5 handle not initialized"
            )

        vol = volume or self.default_volume
        if self.max_volume and vol > self.max_volume:
            return ExecutionResult(
                ExecutionStatus.REJECTED,
                f"Volume {vol} exceeds max_volume {self.max_volume}",
            )

        deal_type = mt5.ORDER_TYPE_BUY if side == Side.BUY else mt5.ORDER_TYPE_SELL
        request = _build_market_request(
            mt5_sym, side, vol, sl=sl, tp=tp,
            deal_type=deal_type, magic=self.magic, comment=comment,
        )

        result = self.mt5_feed.order_send(request)
        if result is None:
            return ExecutionResult(ExecutionStatus.ERROR, "MT5 returned no result")
        if result.get("retcode") not in (RETCODE_DONE, RETCODE_DONE_PARTIAL):
            return ExecutionResult(
                ExecutionStatus.REJECTED,
                f"MT5 rejected order: retcode={result.get('retcode')}. "
                f"Enable 'Algo Trading' in MT5 and check terminal.",
            )

        self._last_order_time = time.time()
        self._last_ticket = result.get("deal") or 0
        return ExecutionResult(
            ExecutionStatus.OK,
            f"{'BUY' if side == Side.BUY else 'SELL'} {vol} {mt5_sym} "
            f"@ ~{result.get('price', 0.0):.2f}",
            order_ticket=result.get("order") or 0,
            deal_ticket=result.get("deal") or 0,
            filled_price=result.get("price") or 0.0,
        )

    def close_position(self, internal: str, comment: str = "orderflow-close") -> ExecutionResult:
        """Close the open position on this instrument (if any)."""
        if not self.is_auto_enabled(internal):
            return ExecutionResult(
                ExecutionStatus.DISABLED,
                f"Auto-trade disabled for {internal}",
            )

        mt5_sym = self.mt5_feed.get_mt5_symbol(internal)
        mt5 = getattr(self.mt5_feed, "_mt5", None)
        if not mt5_sym or mt5 is None:
            return ExecutionResult(
                ExecutionStatus.NO_HANDLE, "MT5 handle/symbol unavailable"
            )

        try:
            positions = mt5.positions_get(symbol=mt5_sym)
        except Exception as exc:
            logger.error(f"positions_get failed: {exc}")
            return ExecutionResult(ExecutionStatus.ERROR, f"positions_get failed: {exc}")

        if positions is None or len(positions) == 0:
            return ExecutionResult(ExecutionStatus.REJECTED, f"No open position on {mt5_sym}")

        # Close the most recent position on this symbol.
        pos = positions[0]
        side = getattr(pos, "type", None)
        pos_type = mt5.ORDER_TYPE_SELL if side == 0 else mt5.ORDER_TYPE_BUY
        volume = getattr(pos, "volume", self.default_volume)
        ticket = getattr(pos, "ticket", 0)

        request = _build_market_request(
            mt5_sym,
            Side.BUY if side == 0 else Side.SELL,   # close by selling a buy / buying a sell
            volume,
            deal_type=pos_type,
            magic=self.magic,
            comment=comment,
        )
        request["position"] = ticket

        result = self.mt5_feed.order_send(request)
        if result is None:
            return ExecutionResult(ExecutionStatus.ERROR, "MT5 returned no result")
        if result.get("retcode") not in (RETCODE_DONE, RETCODE_DONE_PARTIAL):
            return ExecutionResult(
                ExecutionStatus.REJECTED,
                f"MT5 rejected close: retcode={result.get('retcode')}",
            )
        return ExecutionResult(
            ExecutionStatus.OK,
            f"Closed {volume} {mt5_sym} ({result.get('price', 0.0):.2f})",
            deal_ticket=result.get("deal") or 0,
            filled_price=result.get("price") or 0.0,
        )

    def has_open_position(self, internal: str) -> bool:
        """Return True if there's an open position on this instrument."""
        mt5_sym = self.mt5_feed.get_mt5_symbol(internal)
        mt5 = getattr(self.mt5_feed, "_mt5", None)
        if not mt5_sym or mt5 is None:
            return False
        try:
            positions = mt5.positions_get(symbol=mt5_sym)
            return bool(positions is not None and len(positions) > 0)
        except Exception as exc:
            logger.error(f"positions_get failed: {exc}")
            return False

    def modify_stop_loss(
        self, internal: str, sl: float, tp: float = 0.0, comment: str = "orderflow-sltp"
    ) -> ExecutionResult:
        """Modify the SL/TP on the open position for this instrument."""
        if not self.is_auto_enabled(internal):
            return ExecutionResult(
                ExecutionStatus.DISABLED,
                f"Auto-trade disabled for {internal}",
            )

        mt5_sym = self.mt5_feed.get_mt5_symbol(internal)
        mt5 = getattr(self.mt5_feed, "_mt5", None)
        if not mt5_sym or mt5 is None:
            return ExecutionResult(
                ExecutionStatus.NO_HANDLE, "MT5 handle/symbol unavailable"
            )

        try:
            positions = mt5.positions_get(symbol=mt5_sym)
        except Exception as exc:
            logger.error(f"positions_get failed: {exc}")
            return ExecutionResult(ExecutionStatus.ERROR, f"positions_get failed: {exc}")

        if positions is None or len(positions) == 0:
            return ExecutionResult(ExecutionStatus.REJECTED, f"No open position on {mt5_sym}")

        pos = positions[0]
        ticket = getattr(pos, "ticket", 0)
        request = {
            "action": 7,                 # TRADE_ACTION_SLTP
            "position": ticket,
            "symbol": mt5_sym,
        }
        if sl:
            request["sl"] = float(sl)
        if tp:
            request["tp"] = float(tp)
        if comment:
            request["comment"] = comment

        result = self.mt5_feed.order_send(request)
        if result is None:
            return ExecutionResult(ExecutionStatus.ERROR, "MT5 returned no result")
        if result.get("retcode") not in (RETCODE_DONE, RETCODE_DONE_PARTIAL):
            return ExecutionResult(
                ExecutionStatus.REJECTED,
                f"MT5 rejected SL/TP modify: retcode={result.get('retcode')}",
            )
        return ExecutionResult(
            ExecutionStatus.OK,
            f"Modified SL/TP on {mt5_sym} (SL {sl:.2f}, TP {tp:.2f})",
            deal_ticket=ticket,
            filled_price=result.get("price") or 0.0,
        )

    @property
    def last_ticket(self) -> int:
        return self._last_ticket