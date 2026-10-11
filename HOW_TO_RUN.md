# How to Run — OrderFlow Analysis Pro

This guide covers running the OrderFlow Analysis Pro system on **Linux (Pop!\_OS)
with MetaTrader 5 (MT5) hosted in Wine**, and on **Windows natively**.

The system is cross-platform: it detects the OS at runtime and switches between
the native `MetaTrader5` module (Windows) and a standalone RPyC bridge to a
Wine-hosted MT5 (Linux). No code changes are needed between platforms.

---

## Table of Contents

1. [Prerequisites](#1-prerequisites)
2. [Install Dependencies](#2-install-dependencies)
3. [Configure the System](#3-configure-the-system)
4. [Run on Linux (Wine + MT5)](#4-run-on-linux-wine--mt5)
5. [Run on Windows (Native MT5)](#5-run-on-windows-native-mt5)
6. [Run with Bybit (No MT5 Needed)](#6-run-with-bybit-no-mt5-needed)
7. [Run the Dashboard Only (Demo Mode)](#7-run-the-dashboard-only-demo-mode)
8. [Verify the MT5 Bridge Connection](#8-verify-the-mt5-bridge-connection)
9. [Troubleshooting](#9-troubleshooting)

---

## 1. Prerequisites

- **Python 3.10+**
- **`uv`** (fast Python package manager) — or `pip`
- One data source:
  - **MetaTrader 5 terminal** (MT5 feed) — recommended
  - **Bybit** (free, no account needed)
  - **IB Gateway / TWS** (IB feed)

### Linux-only prerequisites (for MT5 via Wine)

- **Wine** installed (`sudo apt install wine` on Pop!\_OS / Ubuntu)
- **MetaTrader 5** installed into a Wine prefix
- **`mt5server.exe`** (the mt5linux RPC server binary) downloaded from the
  [mt5linux releases](https://github.com/lucas-campagna/mt5linux/releases)

---

## 2. Install Dependencies

```bash
cd ~/project/OrderFlow-Analysis-Pro

# Install base dependencies
uv sync

# Linux only — install the MT5 bridge dependency (rpyc, matching the server)
uv sync --extra mt5-linux
```

> **Windows:** install the native MT5 package instead:
> `pip install MetaTrader5`

---

## 3. Configure the System

All configuration lives in `orderflow_system/config/settings.py`.

### 3.1 Select the data source

```python
# DataSource.MT5   → MetaTrader 5 (real broker data)
# DataSource.BYBIT → Bybit perpetuals (free crypto)
# DataSource.IB    → Interactive Brokers
# DataSource.BOTH  → Run multiple feeds
DATA_SOURCE = DataSource.MT5
```

### 3.2 MT5 connection settings

```python
MT5 = MT5Config(
    login=0,                 # 0 = use the already-logged-in account
    password="",
    server="",
    poll_interval_ms=100,    # Tick polling frequency
    enable_book=True,        # Enable market book (DOM)
    download_history_days=3, # Download M1 history for warmup
    # Linux only — Wine + mt5linux RPC bridge (ignored on Windows):
    bridge_host="127.0.0.1", # mt5server.exe host
    bridge_port=18812,       # mt5server.exe port (must match --port)
    bridge_timeout=300,      # RPC request timeout (seconds)
)
```

### 3.3 Enable the instruments you want

Only `BTCUSDT` is enabled by default in `get_all_configs()`. Uncomment others
as needed (NAS100, XAUUSD, forex, stocks, etc.).

---

## 4. Run on Linux (Wine + MT5)

### Step 1 — Start MT5 in Wine

```bash
wine ~/.wine/drive_c/Program\ Files/MetaTrader\ 5/terminal64.exe
```

Wait until MT5 is fully loaded and logged into your broker account.

### Step 2 — Start the RPC bridge (`mt5server.exe`)

Run it in the **same** Wine prefix as MT5 so it can attach to the running
terminal:

```bash
wine ~/mt5server.exe --port 18812
```

Keep this terminal open.

### Step 3 — Run the system

```bash
cd ~/project/OrderFlow-Analysis-Pro
uv run python -m orderflow_system.main
```

You should see logs like:

```
MT5 backend: standalone RPyC bridge -> Wine-hosted MT5 (Linux)
MT5 connected: Exness-MT5Real37 | Account: 263573992 | Balance: 9336.26
Found alternative symbol: BTCUSDc for BTCUSDT
[BTCUSDT] +4320 M1 bars (total 4320)
Dashboard enabled at http://127.0.0.1:8080
```

### Step 4 — Open the dashboard

Navigate to **http://127.0.0.1:8080** in your browser.

---

## 5. Run on Windows (Native MT5)

### Step 1 — Install the native MT5 package

```bash
pip install MetaTrader5
```

### Step 2 — Start MT5

Open the MetaTrader 5 terminal and log into your broker account.

### Step 3 — Run the system

```bash
cd ~/project/OrderFlow-Analysis-Pro
uv run python -m orderflow_system.main
```

The bridge detects Windows and uses the native `MetaTrader5` module directly —
no Wine or `mt5server.exe` needed.

---

## 6. Run with Bybit (No MT5 Needed)

```bash
# 1. Set DATA_SOURCE = DataSource.BYBIT in config/settings.py
# 2. Run the system
uv run python -m orderflow_system.main
```

---

## 7. Run the Dashboard Only (Demo Mode)

```bash
# Run dashboard with deterministic demo data
uv run python -m orderflow_system.dashboard
```

---

## 8. Verify the MT5 Bridge Connection

Quick sanity check that the bridge can reach the Wine-hosted MT5:

```bash
uv run python -c "
from orderflow_system.data.mt5_bridge import create_mt5_handle
mt5 = create_mt5_handle(host='127.0.0.1', port=18812)
print('initialize:', mt5.initialize())
acct = mt5.account_info()
print('server:', acct.server, '| login:', acct.login, '| balance:', acct.balance)
mt5.shutdown()
"
```

Expected output:

```
initialize: True
server: Exness-MT5Real37 | login: 263573992 | balance: 9336.26
```

---

## 9. Troubleshooting

### `mt5linux package not installed` / `rpyc package not installed`

Re-run `uv sync --extra mt5-linux`.

### `initialize: False`

Make sure **both** MT5 (Step 1) and `mt5server.exe` (Step 2) are running, and
that `bridge_port` matches the `--port` you passed to `mt5server.exe`.

### `invalid message type: 18` (rpyc protocol mismatch)

The `mt5server.exe` binary bundles its own rpyc version. The published server
binaries (server-1.0.0 / 1.0.11 / 1.1.1) bundle **rpyc 6.x**. Your client's rpyc
must match:

- `server-1.1.1` / `1.0.11` / `1.0.0` → `pip install "rpyc>=6"`
- older 0.x server binaries → `pip install "rpyc==5.2.3"`

### `Could not find any matching symbol for <SYMBOL>`

Your broker may not offer that instrument, or uses a different symbol suffix.
The feed auto-discovers common alternatives (e.g. `BTCUSDm` → `BTCUSDc` on
Exness). Check available symbols with:

```bash
uv run python -c "
from orderflow_system.data.mt5_bridge import create_mt5_handle
mt5 = create_mt5_handle(host='127.0.0.1', port=18812)
mt5.initialize()
print([s.name for s in mt5.symbols_get()])
mt5.shutdown()
"
```

### `ValueError: no active connection` on shutdown

This is a benign race when the process is killed (e.g. `timeout`/SIGTERM) while
the periodic task is flushing tick buffers. It does not affect normal operation.

### Dashboard not loading

Ensure `DASHBOARD.enabled = True` in config and that port `8080` is free.
