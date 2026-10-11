"""
Cross-platform MetaTrader 5 bridge.

MT5 is a Windows-native application. On Linux we host it inside a Wine
prefix and expose it to native Python via the `mt5linux` RPC bridge
(`mt5server.exe` running under Wine, connected over a local socket).

This module provides a single factory that returns a *drop-in* MT5 handle
with the same method names and constants on both platforms:

  - Windows  -> the native `MetaTrader5` module (import MetaTrader5 as mt5)
  - Linux    -> a standalone RPyC client that talks to `mt5server.exe`

Both expose the same API surface used by the rest of the codebase
(`initialize`, `copy_ticks_from`, `copy_rates_range`, `market_book_get`,
`symbol_info`, `symbols_get`, `symbol_select`, `account_info`, `last_error`,
`shutdown`, `market_book_add`, `market_book_release`, plus constants such as
`COPY_TICKS_ALL` and `TIMEFRAME_M1`).

IMPORTANT — rpyc version must match the server binary:
  `mt5server.exe` bundles its own rpyc. The published binaries (server-1.0.0,
  1.0.11, 1.1.1) bundle rpyc 6.x, while the mt5linux 0.x client used rpyc 5.2.3.
  The rpyc 5.x and 6.x wire protocols are NOT compatible, so the client's rpyc
  version must match the server's. This module connects directly with the
  installed `rpyc` package, so install the rpyc version that matches your
  `mt5server.exe`:
    - server-1.1.1 / 1.0.11 / 1.0.0  ->  pip install "rpyc>=6"
    - older 0.x server binaries      ->  pip install "rpyc==5.2.3"

Usage:
    from orderflow_system.data.mt5_bridge import create_mt5_handle

    mt5 = create_mt5_handle(host="127.0.0.1", port=18812)
    if not mt5.initialize():
        print(mt5.last_error())
    ticks = mt5.copy_ticks_from("XAUUSD", from_dt, 1000, mt5.COPY_TICKS_ALL)
    mt5.shutdown()
"""

from __future__ import annotations

import logging
import platform
import sys
from typing import Any, Optional

logger = logging.getLogger(__name__)


def is_linux() -> bool:
    """True when running on a Linux host (the Wine + mt5linux scenario)."""
    return sys.platform.startswith("linux")


def is_windows() -> bool:
    """True when running natively on Windows."""
    return sys.platform == "win32"


def _create_native_handle() -> Any:
    """Import and return the native MetaTrader5 module (Windows)."""
    try:
        import MetaTrader5 as mt5  # type: ignore
    except ImportError as exc:  # pragma: no cover - depends on platform
        raise RuntimeError(
            "MetaTrader5 package not installed. Install with: "
            "pip install MetaTrader5"
        ) from exc
    return mt5


class _StandaloneMT5:
    """
    Drop-in MT5 handle backed by a direct RPyC connection to `mt5server.exe`.

    `mt5server.exe` runs inside the Wine prefix and exposes the native
    `MetaTrader5` module over RPyC. This class connects to it directly and
    forwards every call, so it works with any `mt5server.exe` binary as long
    as the installed `rpyc` version matches the one bundled in the server.

    It mirrors the native module's method names and constants, so callers
    (e.g. `mt5_feed.py`) need no platform branching.
    """

    # MT5 tick flags (match the native MetaTrader5 module)
    TICK_FLAG_BID = 0x02
    TICK_FLAG_ASK = 0x04
    TICK_FLAG_LAST = 0x08
    TICK_FLAG_VOLUME = 0x10
    TICK_FLAG_BUY = 0x20
    TICK_FLAG_SELL = 0x40

    # Copy flags
    COPY_TICKS_ALL = -1
    COPY_TICKS_INFO = 1
    COPY_TICKS_TRADE = 2

    # Timeframes
    TIMEFRAME_M1 = 1
    TIMEFRAME_M5 = 5
    TIMEFRAME_M15 = 15
    TIMEFRAME_M30 = 30
    TIMEFRAME_H1 = 16385
    TIMEFRAME_H4 = 16388
    TIMEFRAME_D1 = 16408

    # Order types
    ORDER_TYPE_BUY = 0
    ORDER_TYPE_SELL = 1
    ORDER_TYPE_BUY_LIMIT = 2
    ORDER_TYPE_SELL_LIMIT = 3
    ORDER_TYPE_BUY_STOP = 4
    ORDER_TYPE_SELL_STOP = 5

    # Order filling modes
    ORDER_FILLING_FOK = 0
    ORDER_FILLING_IOC = 1
    ORDER_FILLING_RETURN = 2

    # Trade actions
    TRADE_ACTION_DEAL = 1
    TRADE_ACTION_SLTP = 6
    TRADE_ACTION_MODIFY = 7
    TRADE_ACTION_REMOVE = 8

    # Order time policies
    ORDER_TIME_GTC = 0
    ORDER_TIME_DAY = 1

    # Position direction
    POSITION_TYPE_BUY = 0
    POSITION_TYPE_SELL = 1

    def __init__(self, host: str, port: int, timeout: int = 300):
        import rpyc
        from rpyc.core import SlaveService
        from rpyc.utils import factory

        # rpyc 6.x: classic.connect() no longer accepts service=, so use the
        # underlying factory.connect() with the classic SlaveService (which is
        # what mt5server.exe runs). This is version-agnostic across rpyc 5/6.
        self._conn = factory.connect(host, port, service=SlaveService)
        self._conn._config["sync_request_timeout"] = timeout
        # In rpyc 6.x, execute/eval live on the SlaveService root.
        self._root = self._conn.root
        # The server exposes the native MetaTrader5 module as `mt5`.
        self._root.execute("import MetaTrader5 as mt5")
        self._root.execute("import datetime")

    def _eval(self, code: str) -> Any:
        """Evaluate on the server and return the netref proxy directly.

        Used for namedtuple results (AccountInfo, BookInfo, symbol_info) that
        are defined in the server's MetaTrader5 module and cannot be pickled
        back to the client. Attribute access works through the proxy.
        """
        return self._root.eval(code)

    def _eval_obtain(self, code: str) -> Any:
        """Evaluate on the server and materialize the result locally.

        Used for numpy arrays (ticks/rates) and plain scalars, which pickle
        cleanly back to the client.
        """
        import rpyc
        return rpyc.classic.obtain(self._root.eval(code))

    def initialize(self, *args, **kwargs) -> bool:
        return self._eval_obtain(f"mt5.initialize(*{args},**{kwargs})")

    def shutdown(self, *args, **kwargs) -> None:
        return self._eval_obtain(f"mt5.shutdown(*{args},**{kwargs})")

    def last_error(self, *args, **kwargs):
        return self._eval_obtain(f"mt5.last_error(*{args},**{kwargs})")

    def account_info(self, *args, **kwargs):
        return self._eval(f"mt5.account_info(*{args},**{kwargs})")

    def terminal_info(self, *args, **kwargs):
        return self._eval(f"mt5.terminal_info(*{args},**{kwargs})")

    def symbol_info(self, symbol, *args, **kwargs):
        return self._eval(f"mt5.symbol_info({symbol!r},*{args},**{kwargs})")

    def symbols_get(self, *args, **kwargs):
        return self._eval(f"mt5.symbols_get(*{args},**{kwargs})")

    def symbol_select(self, symbol, enable, *args, **kwargs):
        return self._eval_obtain(
            f"mt5.symbol_select({symbol!r},{enable!r},*{args},**{kwargs})"
        )

    def market_book_add(self, symbol, *args, **kwargs):
        return self._eval_obtain(
            f"mt5.market_book_add({symbol!r},*{args},**{kwargs})"
        )

    def market_book_get(self, symbol, *args, **kwargs):
        return self._eval(f"mt5.market_book_get({symbol!r},*{args},**{kwargs})")

    def market_book_release(self, symbol, *args, **kwargs):
        return self._eval_obtain(
            f"mt5.market_book_release({symbol!r},*{args},**{kwargs})"
        )

    def copy_ticks_from(self, symbol, date_from, count, flags, *args, **kwargs):
        code = (
            f"mt5.copy_ticks_from({symbol!r}, {date_from!r}, "
            f"{count!r}, {flags!r}, *{args}, **{kwargs})"
        )
        return self._eval_obtain(code)

    def copy_ticks_range(self, symbol, date_from, date_to, flags, *args, **kwargs):
        code = (
            f"mt5.copy_ticks_range({symbol!r}, {date_from!r}, "
            f"{date_to!r}, {flags!r}, *{args}, **{kwargs})"
        )
        return self._eval_obtain(code)

    def copy_rates_range(self, symbol, timeframe, date_from, date_to, *args, **kwargs):
        code = (
            f"mt5.copy_rates_range({symbol!r}, {timeframe!r}, "
            f"{date_from!r}, {date_to!r}, *{args}, **{kwargs})"
        )
        return self._eval_obtain(code)

    def copy_rates_from(self, symbol, timeframe, date_from, count, *args, **kwargs):
        code = (
            f"mt5.copy_rates_from({symbol!r}, {timeframe!r}, "
            f"{date_from!r}, {count!r}, *{args}, **{kwargs})"
        )
        return self._eval_obtain(code)

    # ── Trading / order execution ──
    # These forward to the native MetaTrader5 module on the server. The
    # request dicts are plain Python dicts, so they pickle cleanly.

    def order_send(self, request, *args, **kwargs):
        """Send a trade request (market/limit/stop). Returns an OrderSendResult."""
        return self._eval_obtain(
            f"mt5.order_send({request!r}, *{args}, **{kwargs})"
        )

    def order_check(self, request, *args, **kwargs):
        """Validate a trade request without sending it."""
        return self._eval_obtain(
            f"mt5.order_check({request!r}, *{args}, **{kwargs})"
        )

    def positions_get(self, *args, **kwargs):
        """Return open positions (optionally filtered by symbol/ticket)."""
        return self._eval_obtain(f"mt5.positions_get(*{args}, **{kwargs})")

    def orders_get(self, *args, **kwargs):
        """Return active pending orders."""
        return self._eval_obtain(f"mt5.orders_get(*{args}, **{kwargs})")

    def history_deals_get(self, *args, **kwargs):
        """Return historical deals."""
        return self._eval_obtain(f"mt5.history_deals_get(*{args}, **{kwargs})")


def _create_standalone_handle(
    host: str,
    port: int,
    timeout: int,
) -> Any:
    """
    Create a standalone RPyC client for a Wine-hosted `mt5server.exe`.

    Connects directly to the running server (no docker/udocker needed) and
    returns a drop-in MT5 handle. The installed `rpyc` version must match the
    one bundled in `mt5server.exe` (see module docstring).
    """
    try:
        import rpyc  # noqa: F401
    except ImportError as exc:  # pragma: no cover - depends on platform
        raise RuntimeError(
            "rpyc package not installed. Install with: pip install rpyc"
        ) from exc

    logger.info(
        "Using standalone RPyC bridge -> Wine-hosted MT5 "
        f"({host}:{port}, timeout={timeout}s)"
    )
    return _StandaloneMT5(host=host, port=port, timeout=timeout)


def create_mt5_handle(
    host: str = "127.0.0.1",
    port: int = 18812,
    timeout: int = 300,
    login: Optional[int] = None,
    password: Optional[str] = None,
    server: Optional[str] = None,
) -> Any:
    """
    Return a drop-in MT5 handle for the current platform.

    On Windows this is the native `MetaTrader5` module. On Linux it is a
    standalone RPyC client that talks to `mt5server.exe` running inside a
    Wine prefix.

    The returned object exposes the same method names and constants used
    throughout the codebase, so callers do not need to branch on platform.
    """
    if is_windows():
        return _create_native_handle()

    if is_linux():
        return _create_standalone_handle(
            host=host,
            port=port,
            timeout=timeout,
        )

    # Fallback for other POSIX systems (macOS etc.) — try native first,
    # then the standalone RPyC bridge.
    try:
        return _create_native_handle()
    except RuntimeError:
        return _create_standalone_handle(
            host=host,
            port=port,
            timeout=timeout,
        )


def describe_backend() -> str:
    """Human-readable description of the active MT5 backend."""
    if is_windows():
        return "native MetaTrader5 module (Windows)"
    if is_linux():
        return "standalone RPyC bridge -> Wine-hosted MT5 (Linux)"
    return f"auto-detect on {platform.system()}"