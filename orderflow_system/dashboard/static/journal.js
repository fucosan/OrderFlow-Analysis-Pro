/**
 * Trade Journal page logic.
 * Loads recorded trades from /api/journal and renders a table + stats.
 */
(function () {
  "use strict";

  const state = {
    trades: [],
    instrumentFilter: "",
    outcomeFilter: "",
    search: "",
  };

  const el = {
    totalCount: document.getElementById("totalCount"),
    tbody: document.getElementById("journalTbody"),
    instrumentFilter: document.getElementById("instrumentFilter"),
    outcomeFilter: document.getElementById("outcomeFilter"),
    searchInput: document.getElementById("searchInput"),
    statTotal: document.getElementById("statTotal"),
    statWins: document.getElementById("statWins"),
    statLosses: document.getElementById("statLosses"),
    statWinRate: document.getElementById("statWinRate"),
    statNetPnL: document.getElementById("statNetPnL"),
    btnRefresh: document.getElementById("btnRefresh"),
    btnExport: document.getElementById("btnExport"),
  };

  function fmtPrice(p) {
    if (p === null || p === undefined || p === 0) return "—";
    const v = Number(p);
    const digits = Math.abs(v) < 1 ? 5 : Math.abs(v) < 1000 ? 2 : 2;
    return v.toFixed(digits);
  }

  function fmtTime(ms) {
    if (!ms) return "—";
    const d = new Date(ms);
    if (isNaN(d)) return "—";
    return d.toLocaleString();
  }

  function fmtPnL(pnl) {
    const v = Number(pnl || 0);
    const cls = v > 0 ? "pos" : v < 0 ? "neg" : "";
    return `<span class="${cls}">${v > 0 ? "+" : ""}${v.toFixed(1)}</span>`;
  }

  function fmtDir(d) {
    const lower = (d || "").toLowerCase();
    const cls = lower === "buy" || lower === "long" ? "dir-long" : "dir-short";
    return `<span class="${cls}">${(d || "—").toUpperCase()}</span>`;
  }

  async function loadInstruments() {
    try {
      const resp = await fetch("/api/journal/instruments");
      const data = await resp.json();
      const list = data.instruments || [];
      el.instrumentFilter.innerHTML =
        '<option value="">All instruments</option>' +
        list.map((i) => `<option value="${i}">${i}</option>`).join("");
    } catch (e) {
      console.error("Failed to load instruments:", e);
    }
  }

  async function loadTrades() {
    try {
      const resp = await fetch("/api/journal?limit=1000");
      const data = await resp.json();
      state.trades = data.trades || [];
      renderStats();
      render();
      el.totalCount.textContent = `Showing ${filtered().length} of ${state.trades.length} trades`;
    } catch (e) {
      console.error("Failed to load journal:", e);
      el.tbody.innerHTML = `<tr class="empty-row"><td colspan="12">Failed to load journal: ${e}</td></tr>`;
    }
  }

  function filtered() {
    const q = state.search.toLowerCase();
    return state.trades.filter((t) => {
      if (state.instrumentFilter && t.instrument !== state.instrumentFilter)
        return false;
      if (state.outcomeFilter === "win" && Number(t.pnl_ticks || 0) <= 0)
        return false;
      if (state.outcomeFilter === "loss" && Number(t.pnl_ticks || 0) >= 0)
        return false;
      if (state.outcomeFilter === "breakeven" && Number(t.pnl_ticks || 0) !== 0)
        return false;
      if (
        q &&
        !(
          (t.notes || "").toLowerCase().includes(q) ||
          (t.instrument || "").toLowerCase().includes(q)
        )
      )
        return false;
      return true;
    });
  }

  function renderStats() {
    let wins = 0,
      losses = 0,
      be = 0,
      net = 0;
    state.trades.forEach((t) => {
      const pnl = Number(t.pnl_ticks || 0);
      net += pnl;
      if (pnl > 0) wins++;
      else if (pnl < 0) losses++;
      else be++;
    });
    const total = wins + losses + be;
    const winRate = total > 0 ? ((wins / total) * 100).toFixed(1) : "0";

    el.statTotal.textContent = total;
    el.statWins.textContent = wins;
    el.statLosses.textContent = losses;
    el.statWinRate.textContent = winRate + "%";
    el.statNetPnL.textContent = (net > 0 ? "+" : "") + net.toFixed(2);
    el.statNetPnL.className =
      "stat-value " + (net > 0 ? "profit" : net < 0 ? "loss" : "");
  }

  function render() {
    const rows = filtered();
    if (rows.length === 0) {
      el.tbody.innerHTML = `<tr class="empty-row"><td colspan="12">No trades match the current filter.</td></tr>`;
      return;
    }
    el.tbody.innerHTML = rows
      .map(
        (t, i) => `
            <tr>
                <td>${t.id}</td>
                <td class="cell-instrument">${t.instrument || "—"}</td>
                <td>${fmtDir(t.direction)}</td>
                <td>${fmtPrice(t.entry_price)}</td>
                <td>${fmtPrice(t.exit_price)}</td>
                <td>${fmtPrice(t.stop_loss)}</td>
                <td>${fmtPrice(t.take_profit)}</td>
                <td>${fmtPnL(t.pnl_ticks)}</td>
                <td>${Number(t.rr_ratio || 0).toFixed(2)}</td>
                <td>${fmtTime(t.entry_time_ms)}</td>
                <td>${fmtTime(t.exit_time_ms)}</td>
                <td class="cell-notes">${t.notes || "—"}</td>
            </tr>
        `,
      )
      .join("");
  }

  function exportCSV() {
    const rows = filtered();
    if (rows.length === 0) {
      alert("No rows to export.");
      return;
    }
    const cols = [
      "id",
      "instrument",
      "direction",
      "entry_price",
      "exit_price",
      "stop_loss",
      "take_profit",
      "pnl_ticks",
      "rr_ratio",
      "entry_time_ms",
      "exit_time_ms",
      "notes",
    ];
    const csv = [
      cols.join(","),
      ...rows.map((t) =>
        cols
          .map((c) => `"${(t[c] ?? "").toString().replace(/"/g, '""')}"`)
          .join(","),
      ),
    ].join("\n");
    const blob = new Blob([csv], { type: "text/csv" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = "trade_journal.csv";
    a.click();
    URL.revokeObjectURL(url);
  }

  el.instrumentFilter.addEventListener("change", () => {
    state.instrumentFilter = el.instrumentFilter.value;
    el.totalCount.textContent = `Showing ${filtered().length} of ${state.trades.length} trades`;
    render();
  });
  el.outcomeFilter.addEventListener("change", () => {
    state.outcomeFilter = el.outcomeFilter.value;
    el.totalCount.textContent = `Showing ${filtered().length} of ${state.trades.length} trades`;
    render();
  });
  el.searchInput.addEventListener("input", () => {
    state.search = el.searchInput.value;
    el.totalCount.textContent = `Showing ${filtered().length} of ${state.trades.length} trades`;
    render();
  });
  el.btnRefresh.addEventListener("click", loadTrades);
  el.btnExport.addEventListener("click", exportCSV);

  loadInstruments();
  loadTrades();
})();
