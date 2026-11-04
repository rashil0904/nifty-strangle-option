"""
build_pnl.py -- generates nifty-strangle-options-pnl.xlsx, a P&L tracker for
the 4 live strategies (Nifty/Sensex strangle + breakdown), read from
results/trade_history.json (one record per closed leg, appended by each
strategy script's exit_all() via results/trade_history.append_closed_leg).

Same pattern as the sibling nifty-option-btst project's results/build_pnl.py
(itself based on volume-daily's build_pnl_simple.py): every *aggregate* cell
(Gross P&L sum, win rate, profit factor, etc.) is a spreadsheet formula over
the Trade Log's Gross P&L column -- openpyxl never pre-computes those --
so the workbook keeps recalculating once it's a Google Sheet. The one
deliberate deviation from that rule: each row's own Gross P&L cell IS a
plain value, not a formula, because leg_lifecycle.py's realized_pnl is
already the complete, pre-aggregated total for that leg's whole cycle
(every stop-loss + final square-off), not something derivable from the two
plain entry/exit price columns a row shows -- there's nothing in the row
for a formula to recompute it from. An "error" row's Gross P&L is left
blank instead (see trade_history.append_closed_leg), so SUM/COUNT-based
formulas naturally skip it rather than treating it as a confirmed 0.

This project differs from BTST in having 4 concurrent strategies rather
than one, so beyond BTST's KPI-card dashboard this adds a small "By
Strategy" table (Gross P&L / Trades / Win Rate per strategy), using the
same SUMIFS/COUNTIFS-over-the-Trade-Log-column approach.

TRACK_SINCE excludes every leg entered before 2026-11-02 from the Sheet
(trade_history.json itself keeps the full permanent ledger regardless) --
that's the date this P&L tracker itself went live, so there's nothing
meaningful to show before it.

Google Drive sync: identical mechanism to nifty-option-btst's -- after a
successful save, shells out to `rclone copy` targeting the same remote
folder ("gdrive:Strategy PnL/") used by every other live strategy on this
account, so Drive treats a re-upload of this filename as an in-place
overwrite of the same Sheet once it exists (auto-converted from the first
xlsx upload). Never pushed if the build itself failed, and a push failure
(rclone not installed, remote not configured, network) only warns --
never raises past main().

There's deliberately no cron here -- this is called from inside
common/leg_lifecycle.py's shared event functions and each strategy
script's own place_entry()/exit_all(), every one of those wrapped in a
try/except so a reporting bug can never break a live trading script. You
can also just run it manually any time:

Usage:
    python3 results/build_pnl.py
"""

import subprocess
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment
from openpyxl.utils import get_column_letter
from openpyxl.formatting.rule import CellIsRule, FormulaRule, DataBarRule

from results.trade_history import load_trade_history

RESULTS_DIR = Path(__file__).resolve().parent
OUT_PATH = RESULTS_DIR / "nifty-strangle-options-pnl.xlsx"
DRIVE_REMOTE_PATH = "gdrive:Strategy PnL/"

TRACK_SINCE = date(2026, 11, 2)

STRATEGIES = ["Nifty Strangle", "Sensex Strangle", "Nifty Breakdown", "Sensex Breakdown"]

# ── Number formats ───────────────────────────────────────────────────────────
INR = '₹#,##0.00;(₹#,##0.00);"-"'
PCT = '0.00%;(0.00%);"-"'
NUM = '#,##0;(#,##0);"-"'
XMULT = '0.00"x"'
DATE = "yyyy-mm-dd"
PRICE = "#,##0.00"

# ── Fonts / fills ─────────────────────────────────────────────────────────────
FONT_NAME = "Arial"
HEADER_FONT = Font(name=FONT_NAME, bold=True, color="FFFFFFFF")
HEADER_FILL = PatternFill(start_color="FF1F3864", end_color="FF1F3864", fill_type="solid")
FORMULA_FONT = Font(name=FONT_NAME, color="FF000000")
LABEL_FONT = Font(name=FONT_NAME, color="FF000000", bold=True)
TITLE_FONT = Font(name=FONT_NAME, bold=True, size=14, color="FF1F3864")
SUBTITLE_FONT = Font(name=FONT_NAME, italic=True, size=10, color="FF808080")
RED_FONT = Font(name=FONT_NAME, color="FFFF0000")
CENTER = Alignment(horizontal="center", vertical="center")
LEFT = Alignment(horizontal="left", vertical="center")


def set_title_subtitle(ws, title: str, subtitle: str, ncols: int) -> None:
    last_col = get_column_letter(ncols)
    ws.merge_cells(f"A1:{last_col}1")
    c = ws["A1"]; c.value = title; c.font = TITLE_FONT; c.alignment = LEFT
    ws.row_dimensions[1].height = 22

    ws.merge_cells(f"A2:{last_col}2")
    c = ws["A2"]; c.value = subtitle; c.font = SUBTITLE_FONT; c.alignment = LEFT
    ws.row_dimensions[2].height = 16


def style_header_row(ws, row: int, ncols: int) -> None:
    for col in range(1, ncols + 1):
        cell = ws.cell(row=row, column=col)
        cell.font = HEADER_FONT
        cell.fill = HEADER_FILL
        cell.alignment = CENTER


def style_formula(cell, number_format: str | None = None) -> None:
    cell.font = FORMULA_FONT
    if number_format:
        cell.number_format = number_format


# ── Trade Log ─────────────────────────────────────────────────────────────────
TL_FIRST_ROW = 4
TL_HEADERS = [
    "Strategy", "Leg", "Tradingsymbol", "Strike", "Option Type",
    "Entry Date", "Entry Price", "Exit Date", "Status",
    "Stop Count", "Re-entry Count", "Quantity", "Lots", "Gross P&L",
]


def _trade_row_values(trade: dict) -> dict:
    return {
        "A": trade.get("strategy", ""), "B": trade.get("leg_key", ""),
        "C": trade.get("tradingsymbol", ""), "D": trade.get("strike", ""),
        "E": trade.get("option_type", ""), "F": trade.get("entry_date", ""),
        "G": trade.get("entry_price", ""), "H": trade.get("exit_date", ""),
        "I": trade.get("status", ""), "J": trade.get("stop_count", ""),
        "K": trade.get("reentry_count", ""), "L": trade.get("quantity", ""),
        "M": trade.get("lots", ""),
    }


def build_trade_log(ws, trades: list[dict]) -> int:
    """Returns the last row written (TL_FIRST_ROW - 1 if no trades)."""
    set_title_subtitle(
        ws, "Trade Log",
        f"One row per closed leg (call or put), entered on/after {TRACK_SINCE.isoformat()} -- "
        "regenerated from results/trade_history.json by results/build_pnl.py.",
        len(TL_HEADERS),
    )

    for col, header in enumerate(TL_HEADERS, start=1):
        ws.cell(row=3, column=col, value=header)
    style_header_row(ws, 3, len(TL_HEADERS))

    widths = [16, 6, 20, 9, 11, 12, 13, 12, 11, 10, 13, 10, 7, 13]
    for col, w in enumerate(widths, start=1):
        ws.column_dimensions[get_column_letter(col)].width = w

    manual_fmts = {"D": NUM, "F": DATE, "G": PRICE, "H": DATE, "J": NUM, "K": NUM, "L": NUM, "M": NUM}

    last_row = TL_FIRST_ROW - 1
    for offset, trade in enumerate(trades):
        r = TL_FIRST_ROW + offset
        last_row = r
        row_vals = _trade_row_values(trade)
        for col_letter, val in row_vals.items():
            cell = ws[f"{col_letter}{r}"]
            cell.value = val
            if col_letter in manual_fmts:
                cell.number_format = manual_fmts[col_letter]

        gross_pnl = trade.get("gross_pnl")
        cell_n = ws[f"N{r}"]
        if gross_pnl is None:
            cell_n.value = "(check Jainam manually)"
        else:
            cell_n.value = gross_pnl
            style_formula(cell_n, INR)

    if trades:
        ws.conditional_formatting.add(
            f"N{TL_FIRST_ROW}:N{last_row}",
            CellIsRule(operator="lessThan", formula=["0"], font=RED_FONT),
        )
        ws.auto_filter.ref = f"A3:N{last_row}"

    ws.freeze_panes = "C4"
    return last_row


# ── Total PnL (dashboard) ────────────────────────────────────────────────────
BAND_FONT = Font(name=FONT_NAME, bold=True, size=11, color="FF1F3864")
BAND_FILL = PatternFill(start_color="FFD9E2F3", end_color="FFD9E2F3", fill_type="solid")
CARD_LABEL_FONT = Font(name=FONT_NAME, size=9, color="FFFFFFFF")
CARD_NUM_FONT = Font(name=FONT_NAME, size=26, bold=True, color="FFFFFFFF")
GREEN_FILL = PatternFill(start_color="FF2E7D32", end_color="FF2E7D32", fill_type="solid")
RED_FILL = PatternFill(start_color="FFC62828", end_color="FFC62828", fill_type="solid")
CARD_NAVY = "FF1F3864"
CARD_TEAL = "FF1B7A75"
CARD_AMBER = "FFC77D02"
CARD_PURPLE = "FF5B2C83"
DASHBOARD_COLS = 16

CARD_LABEL_ROW = 5
CARD_NUM_TOP = 6
CARD_NUM_BOTTOM = 8
BAND_STATS = 10
BAND_BY_STRATEGY = 19


def _band(ws, row: int, text: str) -> None:
    last_col = get_column_letter(DASHBOARD_COLS)
    ws.merge_cells(f"A{row}:{last_col}{row}")
    cell = ws[f"A{row}"]
    cell.value = text
    cell.font = BAND_FONT
    cell.fill = BAND_FILL
    cell.alignment = Alignment(horizontal="left", vertical="center", indent=1)
    ws.row_dimensions[row].height = 18
    for col in range(1, DASHBOARD_COLS + 1):
        ws.cell(row=row, column=col).fill = BAND_FILL


def _kpi_card(ws, col_start, col_end, label, formula, number_format, fill_hex, conditional) -> None:
    first_col_letter = get_column_letter(col_start)
    last_col_letter = get_column_letter(col_end)
    label_range = f"{first_col_letter}{CARD_LABEL_ROW}:{last_col_letter}{CARD_LABEL_ROW}"
    num_range = f"{first_col_letter}{CARD_NUM_TOP}:{last_col_letter}{CARD_NUM_BOTTOM}"

    ws.merge_cells(label_range)
    ws.merge_cells(num_range)

    fill = PatternFill(start_color=fill_hex, end_color=fill_hex, fill_type="solid")
    for row in (CARD_LABEL_ROW, *range(CARD_NUM_TOP, CARD_NUM_BOTTOM + 1)):
        for col in range(col_start, col_end + 1):
            ws.cell(row=row, column=col).fill = fill

    label_cell = ws[f"{first_col_letter}{CARD_LABEL_ROW}"]
    label_cell.value = label
    label_cell.font = CARD_LABEL_FONT
    label_cell.alignment = CENTER

    num_cell = ws[f"{first_col_letter}{CARD_NUM_TOP}"]
    num_cell.value = formula
    num_cell.font = CARD_NUM_FONT
    num_cell.alignment = CENTER
    num_cell.number_format = number_format

    if conditional:
        anchor = f"${first_col_letter}${CARD_NUM_TOP}"
        for rng in (label_range, num_range):
            ws.conditional_formatting.add(rng, FormulaRule(formula=[f"{anchor}>=0"], fill=GREEN_FILL))
            ws.conditional_formatting.add(rng, FormulaRule(formula=[f"{anchor}<0"], fill=RED_FILL))


def build_total_pnl_dashboard(ws, tl_last_row: int) -> None:
    set_title_subtitle(
        ws, "Total PnL",
        f"Nifty/Sensex strangle + breakdown (since {TRACK_SINCE.isoformat()}) -- "
        "everything below calculates automatically from Trade Log.",
        DASHBOARD_COLS,
    )

    ws.column_dimensions["A"].width = 4
    for group_start in (2, 6, 10, 14):
        for offset in range(3):
            ws.column_dimensions[get_column_letter(group_start + offset)].width = 15
    for gap_col in (5, 9, 13):
        ws.column_dimensions[get_column_letter(gap_col)].width = 8

    tl_last = max(tl_last_row, 4)
    n_range = f"'Trade Log'!$N$4:$N${tl_last}"
    a_range = f"'Trade Log'!$A$4:$A${tl_last}"

    _band(ws, 4, "STRATEGY SNAPSHOT (ALL STRATEGIES)")
    card_defs = [
        (2, 4, "GROSS P&L (₹)", f"=SUM({n_range})", INR, CARD_NAVY, True),
        (6, 8, "WIN RATE", f'=IFERROR(COUNTIF({n_range},">0")/COUNT({n_range}),"")', PCT, CARD_AMBER, False),
        (10, 12, "TOTAL TRADES", f"=COUNT({n_range})", NUM, CARD_TEAL, False),
        (14, 16, "PROFIT FACTOR",
         f'=IFERROR(SUMIF({n_range},">0")/ABS(SUMIF({n_range},"<0")),"")', XMULT, CARD_PURPLE, False),
    ]
    for col_start, col_end, label, formula, fmt, fill_hex, conditional in card_defs:
        _kpi_card(ws, col_start, col_end, label, formula, fmt, fill_hex, conditional)

    _band(ws, BAND_STATS, "DETAILED STATS")
    ws["B11"].value = "Metric"
    ws["C11"].value = "Value"
    for addr in ("B11", "C11"):
        ws[addr].font = HEADER_FONT
        ws[addr].fill = HEADER_FILL
        ws[addr].alignment = CENTER

    stats_rows = [
        (12, "Winning Trades", f'=COUNTIF({n_range},">0")', NUM),
        (13, "Losing Trades", f'=COUNTIF({n_range},"<0")', NUM),
        (14, "Average Win (₹)", f'=IFERROR(AVERAGEIF({n_range},">0"),"")', INR),
        (15, "Average Loss (₹)", f'=IFERROR(AVERAGEIF({n_range},"<0"),"")', INR),
        (16, "Best Trade (₹)", f'=IFERROR(MAX({n_range}),"")', INR),
        (17, "Worst Trade (₹)", f'=IFERROR(MIN({n_range}),"")', INR),
    ]
    for row, label, formula, fmt in stats_rows:
        ws[f"B{row}"].value = label
        ws[f"B{row}"].font = LABEL_FONT
        cell_c = ws[f"C{row}"]
        cell_c.value = formula
        style_formula(cell_c, fmt)

    ws.conditional_formatting.add(
        "C14:C17",
        DataBarRule(start_type="min", start_value=None, end_type="max", end_value=None, color="638EC6"),
    )

    _band(ws, BAND_BY_STRATEGY, "BY STRATEGY")
    headers = ["Strategy", "Gross P&L (₹)", "Trades", "Win Rate"]
    for col, header in enumerate(headers, start=2):
        c = ws.cell(row=BAND_BY_STRATEGY + 1, column=col, value=header)
        c.font = HEADER_FONT
        c.fill = HEADER_FILL
        c.alignment = CENTER

    for offset, strategy in enumerate(STRATEGIES):
        row = BAND_BY_STRATEGY + 2 + offset
        crit = f'"{strategy}"'
        ws[f"B{row}"].value = strategy
        ws[f"B{row}"].font = LABEL_FONT

        pnl_cell = ws[f"C{row}"]
        pnl_cell.value = f"=SUMIFS({n_range},{a_range},{crit})"
        style_formula(pnl_cell, INR)

        # COUNTIFS(...,N_range,"") only matches a truly BLANK cell -- an "error" row's P&L
        # cell holds literal text ("(check Jainam manually)"), not a blank, so that wouldn't
        # exclude it. ISNUMBER() correctly excludes any non-numeric cell regardless of
        # whether it's blank or text, same effect COUNT()/COUNTIF(">0") already gets for free
        # in the dashboard cards above.
        count_cell = ws[f"D{row}"]
        count_cell.value = f"=SUMPRODUCT(({a_range}={crit})*ISNUMBER({n_range}))"
        style_formula(count_cell, NUM)

        win_cell = ws[f"E{row}"]
        win_cell.value = (
            f'=IFERROR(SUMPRODUCT(({a_range}={crit})*ISNUMBER({n_range})*({n_range}>0))/'
            f'SUMPRODUCT(({a_range}={crit})*ISNUMBER({n_range})),"")'
        )
        style_formula(win_cell, PCT)

    ws.conditional_formatting.add(
        f"C{BAND_BY_STRATEGY + 2}:C{BAND_BY_STRATEGY + 1 + len(STRATEGIES)}",
        CellIsRule(operator="lessThan", formula=["0"], font=RED_FONT),
    )

    ws.freeze_panes = "A9"


def _push_to_drive(out_path: Path) -> None:
    """rclone copy to the same remote path every time -- an in-place
    overwrite of the existing nifty-strangle-options-pnl Google Sheet (same
    file id) once it exists, not a new upload each time. Only called after
    a successful save (see main()). Never raises -- a missing rclone
    binary/unconfigured remote/network hiccup should warn, not break
    report generation."""
    try:
        result = subprocess.run(
            ["rclone", "copy", str(out_path), DRIVE_REMOTE_PATH],
            capture_output=True, text=True, timeout=60,
        )
        if result.returncode == 0:
            print(f"    Pushed to {DRIVE_REMOTE_PATH}")
        else:
            print(f"    WARNING: rclone push failed: {result.stderr.strip() or result.stdout.strip()}")
    except Exception as exc:  # noqa: BLE001 -- e.g. rclone not installed
        print(f"    WARNING: rclone push failed ({exc})")


def main(out_path: Path = OUT_PATH) -> None:
    all_trades = load_trade_history()
    trades = [t for t in all_trades if t.get("entry_date") and date.fromisoformat(t["entry_date"]) >= TRACK_SINCE]
    skipped = len(all_trades) - len(trades)

    wb = Workbook()
    ws_total = wb.active
    ws_total.title = "Total PnL"
    ws_log = wb.create_sheet("Trade Log")

    tl_last_row = build_trade_log(ws_log, trades)
    build_total_pnl_dashboard(ws_total, tl_last_row)

    for ws in wb.worksheets:
        ws.sheet_view.showGridLines = False

    wb.save(out_path)
    note = f", {skipped} pre-{TRACK_SINCE.isoformat()} trade(s) excluded" if skipped else ""
    print(f"Wrote {out_path} ({len(trades)} trade(s){note})" if trades
          else f"Wrote {out_path} (no closed trades on/after {TRACK_SINCE.isoformat()} yet{note})")

    _push_to_drive(out_path)


if __name__ == "__main__":
    main()
