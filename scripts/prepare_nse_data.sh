#!/usr/bin/env bash
# Prepare NSE daily data for qlib.
# Usage:
#   bash scripts/prepare_nse_data.sh [START_DATE] [END_DATE] [QLIB_DATA_DIR] [START_IDX] [END_IDX]
#
# Defaults:
#   START_DATE   = 2000-01-01
#   END_DATE     = today
#   QLIB_DATA_DIR = ~/.qlib/qlib_data/nse_data
#   START_IDX    = 0        (first stock, 0-based)
#   END_IDX      = (empty)  = all remaining stocks
#
# Examples:
#   All stocks:        bash prepare_nse_data.sh 2020-01-01 2026-04-05 ~/.qlib/...
#   Stocks 100-200:    bash prepare_nse_data.sh 2020-01-01 2026-04-05 ~/.qlib/... 100 200
#   Stocks 200 to end: bash prepare_nse_data.sh 2020-01-01 2026-04-05 ~/.qlib/... 200

set -euo pipefail

START_DATE="${1:-2000-01-01}"
END_DATE="${2:-$(date +%Y-%m-%d)}"
QLIB_DIR="${3:-$HOME/.qlib/qlib_data/nse_data}"
START_IDX="${4:-0}"
END_IDX="${5:-}"

SOURCE_DIR="$HOME/.qlib/stock_data/source/nse_1d"
NORMALIZE_DIR="$HOME/.qlib/stock_data/normalize/nse_1d"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COLLECTOR="$SCRIPT_DIR/data_collector/yahoo/collector.py"
DUMP_BIN="$SCRIPT_DIR/dump_bin.py"
IN_INDEX="$SCRIPT_DIR/data_collector/in_index/collector.py"

# Use venv python if available, else fall back to python3
PYTHON="${SCRIPT_DIR}/../.venv/bin/python"
if [ ! -x "$PYTHON" ]; then
    PYTHON="$(command -v python3 || command -v python)"
fi
echo "Using Python: $PYTHON"

# Derive offset_nums and limit_nums from START_IDX / END_IDX
OFFSET_ARG=""
LIMIT_ARG=""
if [ "$START_IDX" -gt 0 ]; then
    OFFSET_ARG="--offset_nums $START_IDX"
fi
if [ -n "$END_IDX" ]; then
    LIMIT_NUMS=$(( END_IDX - START_IDX ))
    LIMIT_ARG="--limit_nums $LIMIT_NUMS"
fi

echo "========================================"
echo " NSE Data Preparation"
echo "========================================"
echo "  Start date : $START_DATE"
echo "  End date   : $END_DATE"
echo "  Source dir : $SOURCE_DIR"
echo "  Normalize  : $NORMALIZE_DIR"
echo "  Qlib dir   : $QLIB_DIR"
if [ -n "$END_IDX" ]; then
    echo "  Stocks     : $START_IDX to $END_IDX"
elif [ "$START_IDX" -gt 0 ]; then
    echo "  Stocks     : $START_IDX to end"
else
    echo "  Stocks     : all"
fi
echo "========================================"

# ── Step 1: Download raw NSE daily data from Yahoo Finance ──────────────────
echo ""
echo "[1/5] Downloading NSE daily data from Yahoo Finance..."
if [ -n "$END_IDX" ]; then
    echo "      (stocks $START_IDX to $END_IDX)"
elif [ "$START_IDX" -gt 0 ]; then
    echo "      (stocks $START_IDX to end)"
else
    echo "      (NSE has ~2000 stocks — this may take ~30 min)"
fi
"$PYTHON" "$COLLECTOR" download_data \
    --source_dir   "$SOURCE_DIR" \
    --region       IN \
    --start        "$START_DATE" \
    --end          "$END_DATE" \
    --interval     1d \
    --delay        0.5 \
    --max_workers  1 \
    $OFFSET_ARG \
    $LIMIT_ARG

echo "[1/5] Done. Raw CSVs in: $SOURCE_DIR"

# ── Step 2: Normalize (adjust prices, align to calendar) ────────────────────
echo ""
echo "[2/5] Normalizing data..."
"$PYTHON" "$COLLECTOR" normalize_data \
    --source_dir    "$SOURCE_DIR" \
    --normalize_dir "$NORMALIZE_DIR" \
    --region        IN \
    --interval      1d

echo "[2/5] Done. Normalized CSVs in: $NORMALIZE_DIR"

# ── Step 3: Dump to qlib binary format ──────────────────────────────────────
echo ""
echo "[3/5] Dumping to qlib binary format..."
"$PYTHON" "$DUMP_BIN" dump_all \
    --data_path      "$NORMALIZE_DIR" \
    --qlib_dir       "$QLIB_DIR" \
    --freq           day \
    --exclude_fields date,symbol

echo "[3/5] Done. Qlib data in: $QLIB_DIR"

# ── Step 4: Build Nifty 50 instruments file ─────────────────────────────────
echo ""
echo "[4/5] Building Nifty 50 instruments file..."
"$PYTHON" "$IN_INDEX" \
    --index_name NIFTY50 \
    --qlib_dir   "$QLIB_DIR" \
    --method     parse_instruments

echo "[4/5] Done."

# ── Step 5: Build Nifty 500 instruments file ────────────────────────────────
echo ""
echo "[5/5] Building Nifty 500 instruments file..."
"$PYTHON" "$IN_INDEX" \
    --index_name NIFTY500 \
    --qlib_dir   "$QLIB_DIR" \
    --method     parse_instruments

echo "[5/5] Done."

echo ""
echo "========================================"
echo " All done! Initialise qlib with:"
echo ""
echo "   import qlib"
echo "   qlib.init(provider_uri='$QLIB_DIR')"
echo ""
echo " Available instruments:"
echo "   D.instruments('nifty50')   # Nifty 50 constituents"
echo "   D.instruments('nifty500')  # Nifty 500 constituents"
echo "   D.instruments('all')       # All NSE stocks"
echo "========================================"
