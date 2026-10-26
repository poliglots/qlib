#!/usr/bin/env python3
# Find stocks whose price hit their LOWEST value TODAY or THIS MONTH
# during a given lookback period (1y, 2y, 5y, up to 6y based on available data).
#
# For each stock over a lookback window, finds:
#   1. The lowest price reached during that period
#   2. The date when that low occurred
#
# Then returns stocks whose period-low occurred:
#   TODAY  — the low was hit on the most recent trading day
#   THIS-MONTH — the low was hit within the current calendar month
#
# Usage:
#     python find_lowest_value_stocks.py --period 1y --hit today
#     python find_lowest_value_stocks.py --period 3y --hit this-month --top-k 20
#     python find_lowest_value_stocks.py --periods 1y,2y,3y,5y --hit today --top-k 30
#     python find_lowest_value_stocks.py --periods 1y,2y,5y --hit this-month
#     python find_lowest_value_stocks.py --period 2y --feature adjclose --hit today
#
# Note: Available data spans ~2020-01 to 2026-09 (~6.5 years). Periods beyond 6y
# will use the full available history.

import argparse
import json
import os
import sys
import time
from collections import OrderedDict

import numpy as np

try:
    import torch
    HAS_TORCH = True
except ImportError:
    HAS_TORCH = False

# ── Constants ──────────────────────────────────────────────────────────────
DEFAULT_DATA_DIR = os.path.expanduser("~/.qlib/stock_data/normalize/nse_1d")
DEFAULT_CACHE_DIR = os.path.expanduser("~/.qlib/stock_data/cache")
DEFAULT_FEATURE = "close"
SUPPORTED_FEATURES = ["open", "high", "low", "close", "volume", "adjclose"]

# Trading days per period
TRADING_DAYS = {
    "1m": 21,
    "2m": 42,
    "3m": 63,
    "6m": 126,
    "1y": 252,
}
for _y in range(2, 31):
    TRADING_DAYS[f"{_y}y"] = _y * 252


def load_cached_tensor(feature, cache_dir):
    """Load tensor + metadata from cache, falling back to CSVs."""
    tensor_path = os.path.join(cache_dir, f"tensor_{feature}.npy")
    meta_path = os.path.join(cache_dir, f"meta_{feature}.json")

    if not os.path.exists(tensor_path):
        print("[WARN] Cache not found, building from CSVs...")
        loader = os.path.join(os.path.dirname(os.path.abspath(__file__)), "load_stock_tensors.py")
        if os.path.exists(loader):
            rc = os.system(f"uv run python3 '{loader}' --feature {feature} --cache_dir '{cache_dir}'")
            if rc != 0:
                print("[ERROR] Failed to load data from CSVs")
                sys.exit(1)
        else:
            print("[ERROR] Cannot find load_stock_tensors.py")
            sys.exit(1)

    tensor = np.load(tensor_path)
    with open(meta_path) as f:
        meta = json.load(f)
    return tensor, meta["symbols"], meta["dates"]


def _period_to_days(period_str):
    """Convert period string like '1y', '3y', '10y' to trading days."""
    return TRADING_DAYS.get(period_str)


def find_stocks_near_period_low(tensor, symbols, dates, window, within_pct):
    """
    For each stock over the last `window` days:
      1. Find the minimum price during the window
      2. Find the date of that minimum
      3. Check if current price is within `within_pct`% of that low
      4. Rank by how close to low (distance_pct ascending)

    Returns list of dicts sorted by proximity to low.
    """
    if not HAS_TORCH:
        print("[ERROR] torch required for GPU acceleration")
        sys.exit(1)

    n_stocks, n_dates = tensor.shape
    if window >= n_dates:
        print(f"[ERROR] Window {window} >= {n_dates} dates, cannot compute")
        sys.exit(1)

    t_gpu = torch.from_numpy(tensor).float().cuda()

    # Extract the window slice
    window_data = t_gpu[:, -window:]  # (stocks, window)

    # NaN-aware min: replace NaN with a very large number
    LARGE = torch.full_like(window_data, 1e10)
    window_filled = torch.where(torch.isnan(window_data), LARGE, window_data)
    min_prices, min_indices = torch.min(window_filled, dim=1)
    current_prices = t_gpu[:, -1]

    # Valid: no NaN in current or min, and min > 0
    valid = (~torch.isnan(current_prices)) & (current_prices > 0) & (~torch.isnan(min_prices)) & (min_prices > 0)

    # Check within_pct: current_price <= min_price * (1 + within_pct/100)
    threshold = min_prices * (1 + within_pct / 100)
    is_near = valid & (current_prices <= threshold)

    # Distance from low: (current - min) / min  (negative or zero means at/below low)
    distance_pct = torch.full((n_stocks,), float("nan"), device=t_gpu.device)
    distance_pct[valid] = (current_prices[valid] - min_prices[valid]) / min_prices[valid] * 100

    # Date of low: map argmin to actual date
    low_date_idx = -window + min_indices  # negative index → actual index
    low_dates = [dates[int(idx)] for idx in low_date_idx.cpu().tolist()]
    current_date = dates[-1]

    # Collect results
    results = []
    for i in range(n_stocks):
        if not is_near[i]:
            continue
        results.append({
            "symbol": symbols[i],
            "current_price": float(current_prices[i]),
            "period_low": float(min_prices[i]),
            "low_date": low_dates[i],
            "distance_pct": float(distance_pct[i]),
            "window_days": window,
            "start_date": dates[-window],
            "end_date": current_date,
        })

    # Sort: closest to low first (most negative distance_pct)
    results.sort(key=lambda x: x["distance_pct"])
    return results


def find_lowest_current_prices(tensor, symbols, dates, window):
    """
    Find stocks with the lowest CURRENT prices, but report
    their period-low and how far above it they sit.

    Useful for finding "cheap stocks" with historical context.
    """
    if not HAS_TORCH:
        print("[ERROR] torch required for GPU acceleration")
        sys.exit(1)

    n_stocks, n_dates = tensor.shape
    if window >= n_dates:
        print(f"[ERROR] Window {window} >= {n_dates} dates, cannot compute")
        sys.exit(1)

    t_gpu = torch.from_numpy(tensor).float().cuda()
    window_data = t_gpu[:, -window:]
    current_prices = t_gpu[:, -1]

    # Valid: no NaN, price > 0
    valid = (~torch.isnan(current_prices)) & (current_prices > 0)

    # NaN-aware min
    LARGE = torch.full_like(window_data, 1e10)
    window_filled = torch.where(torch.isnan(window_data), LARGE, window_data)
    min_prices, min_indices = torch.min(window_filled, dim=1)
    min_valid = valid & (~torch.isnan(min_prices)) & (min_prices > 0)

    # Filter valid and sort by current price ascending
    valid_idx = torch.where(valid)[0]
    valid_current = current_prices[valid_idx]

    # Get period low for valid stocks
    period_lows = torch.full((len(valid_idx),), float('nan'))
    low_dates_list = []
    for j, i in enumerate(valid_idx.tolist()):
        wi = int(min_indices[i].item())
        period_lows[j] = float(window_data[i, wi])
        low_dates_list.append(dates[-window + wi])

    # Sort by current price
    sorted_indices = torch.argsort(valid_current)
    current_sorted = valid_current[sorted_indices]
    idx_sorted = valid_idx[sorted_indices]

    results = []
    for j in range(len(current_sorted)):
        i = idx_sorted[j].item()
        dist = (float(current_sorted[j]) - float(period_lows[j])) / float(period_lows[j]) * 100
        results.append({
            "symbol": symbols[i],
            "current_price": float(current_sorted[j]),
            "period_low": float(period_lows[j]),
            "low_date": low_dates_list[j],
            "distance_pct": dist,
            "window_days": window,
            "start_date": dates[-window],
            "end_date": dates[-1],
        })

    return results


def find_lowest_value_across_periods(tensor, symbols, dates, period_strs, within_pct):
    """
    Scan multiple periods simultaneously. For each stock, check if it's
    within `within_pct`% of its low on EACH period.

    Returns stocks that are near-low on ALL requested periods,
    plus per-period details.
    """
    if not HAS_TORCH:
        print("[ERROR] torch required for GPU acceleration")
        sys.exit(1)

    n_stocks, n_dates = tensor.shape
    t_gpu = torch.from_numpy(tensor).float().cuda()
    current_prices = t_gpu[:, -1]
    valid_base = (~torch.isnan(current_prices)) & (current_prices > 0)

    period_results = OrderedDict()
    for p_str in period_strs:
        window = _period_to_days(p_str)
        if not window or window >= n_dates:
            print(f"[WARN] Skipping {p_str}: window {window} too large or invalid")
            continue
        period_results[p_str] = {
            "window": window,
            "window_data": t_gpu[:, -window:],
            "dates": dates,
            "current_prices": current_prices,
            "within_pct": within_pct,
        }

    # For each stock, check proximity to low on all periods
    results = []
    for i in range(n_stocks):
        if not valid_base[i]:
            continue

        stock_current = float(current_prices[i])
        per_period = {}
        all_near = True

        for p_str, pdata in period_results.items():
            window_data = pdata["window_data"]
            LARGE = torch.full_like(window_data, 1e10)
            window_filled = torch.where(torch.isnan(window_data), LARGE, window_data)
            min_p, min_idx = torch.min(window_filled[i:i+1], dim=1)
            min_val = float(min_p[0])
            low_idx = int(min_idx[0])
            low_date = pdata["dates"][-pdata["window"] + low_idx]

            threshold = min_val * (1 + pdata["within_pct"] / 100)
            is_near = stock_current <= threshold
            dist = (stock_current - min_val) / min_val * 100

            if not is_near:
                all_near = False

            per_period[p_str] = {
                "low": min_val,
                "low_date": low_date,
                "distance_pct": round(dist, 2),
                "is_near": is_near,
            }

        if all_near and len(per_period) > 0:
            results.append({
                "symbol": symbols[i],
                "current_price": stock_current,
                "per_period": per_period,
            })

    # Sort by how far from 1y low (best first)
    results.sort(key=lambda x: x["per_period"].get("1y", {}).get("distance_pct", 999))
    return results


def find_stocks_hit_low_today(tensor, symbols, dates, window):
    """
    For each stock over the last `window` days, find the minimum.
    Return only stocks whose LOWEST value occurred TODAY (last trading day).
    """
    if not HAS_TORCH:
        print("[ERROR] torch required for GPU acceleration")
        sys.exit(1)

    n_stocks, n_dates = tensor.shape
    if window >= n_dates:
        print(f"[ERROR] Window {window} >= {n_dates} dates, cannot compute")
        sys.exit(1)

    t_gpu = torch.from_numpy(tensor).float().cuda()
    window_data = t_gpu[:, -window:]

    # NaN-aware min
    LARGE = torch.full_like(window_data, 1e10)
    window_filled = torch.where(torch.isnan(window_data), LARGE, window_data)
    min_prices, min_indices = torch.min(window_filled, dim=1)

    # Today is the last column of the window
    today_prices = t_gpu[:, -1]
    today_idx = window - 1  # index of last column within window

    # Valid: no NaN, price > 0
    valid = (~torch.isnan(today_prices)) & (today_prices > 0) & (~torch.isnan(min_prices)) & (min_prices > 0)

    # Low occurred today: argmin index == last column of window
    hit_today = valid & (min_indices == today_idx)

    current_date = dates[-1]
    start_date = dates[-window]

    results = []
    for i in range(n_stocks):
        if not hit_today[i]:
            continue
        results.append({
            "symbol": symbols[i],
            "current_price": float(today_prices[i]),
            "period_low": float(min_prices[i]),
            "low_date": current_date,
            "window_days": window,
            "start_date": start_date,
            "end_date": current_date,
        })

    # Sort by current price ascending (cheapest first)
    results.sort(key=lambda x: x["current_price"])
    return results


def find_stocks_hit_low_this_month(tensor, symbols, dates, window):
    """
    For each stock over the last `window` days, find the minimum.
    Return only stocks whose LOWEST value occurred THIS MONTH.
    """
    if not HAS_TORCH:
        print("[ERROR] torch required for GPU acceleration")
        sys.exit(1)

    n_stocks, n_dates = tensor.shape
    if window >= n_dates:
        print(f"[ERROR] Window {window} >= {n_dates} dates, cannot compute")
        sys.exit(1)

    t_gpu = torch.from_numpy(tensor).float().cuda()
    window_data = t_gpu[:, -window:]

    # NaN-aware min
    LARGE = torch.full_like(window_data, 1e10)
    window_filled = torch.where(torch.isnan(window_data), LARGE, window_data)
    min_prices, min_indices = torch.min(window_filled, dim=1)

    current_prices = t_gpu[:, -1]

    # Valid
    valid = (~torch.isnan(current_prices)) & (current_prices > 0) & (~torch.isnan(min_prices)) & (min_prices > 0)

    # Determine this month
    current_date = dates[-1]
    current_year, current_month, _ = map(int, current_date.split("-"))

    # Check low_date against this month
    hit_this_month = torch.zeros(n_stocks, dtype=torch.bool, device=t_gpu.device)
    for i in range(n_stocks):
        if not valid[i]:
            continue
        low_idx = int(min_indices[i].item())
        low_date = dates[-window + low_idx]
        ly, lm, _ = map(int, low_date.split("-"))
        if ly == current_year and lm == current_month:
            hit_this_month[i] = True

    current_date = dates[-1]
    start_date = dates[-window]

    results = []
    for i in range(n_stocks):
        if not hit_this_month[i]:
            continue
        results.append({
            "symbol": symbols[i],
            "current_price": float(current_prices[i]),
            "period_low": float(min_prices[i]),
            "low_date": dates[-window + int(min_indices[i].item())],
            "window_days": window,
            "start_date": start_date,
            "end_date": current_date,
        })

    # Sort by current price ascending (cheapest first)
    results.sort(key=lambda x: x["current_price"])
    return results


# ── Display Helpers ────────────────────────────────────────────────────────

def print_near_low_results(results, period_str, within_pct, top_k):
    """Print results in MODE 1 (near-low)."""
    n = len(results)
    print("\n" + "=" * 95)
    print(f"  STOCKS NEAR PERIOD LOW  |  Window: {period_str}  |  Within {within_pct}% of low")
    print("=" * 95)
    print(f"  {'#':<4} {'Symbol':<22s} {'Current':>10s} {'Period Low':>12s}  "
          f"{'Low Date':<12s} {'Distance':>10s}")
    print(f"  {'─'*4} {'─'*22} {'─'*10} {'─'*12}  {'─'*12} {'─'*10}")

    for i, r in enumerate(results[:top_k], 1):
        sign = "" if r["distance_pct"] < 0 else "+"
        print(f"  {i:<4} {r['symbol']:<22s} {r['current_price']:>10.2f} {r['period_low']:>12.2f}  "
              f"{r['low_date']:<12s} {sign}{r['distance_pct']:>9.2f}%")

    print(f"\n{'─'*95}")
    print(f"  Found: {n} stocks  |  Showing top {min(top_k, n)}")
    print(f"  These stocks are at or near their {period_str} low — potential value plays.")
    print("=" * 95 + "\n")


def print_absolute_results(results, period_str, top_k):
    """Print results in MODE 2 (absolute lowest)."""
    n = len(results)
    print("\n" + "=" * 100)
    print(f"  LOWEST PRICED STOCKS TODAY  |  Period-low context: {period_str}")
    print("=" * 100)
    print(f"  {'#':<4} {'Symbol':<22s} {'Current':>10s} {'Period Low':>12s}  "
          f"{'Low Date':<12s} {'Above Low':>10s}")
    print(f"  {'─'*4} {'─'*22} {'─'*10} {'─'*12}  {'─'*12} {'─'*10}")

    for i, r in enumerate(results[:top_k], 1):
        sign = "+" if r["distance_pct"] > 0 else ""
        print(f"  {i:<4} {r['symbol']:<22s} {r['current_price']:>10.2f} {r['period_low']:>12.2f}  "
              f"{r['low_date']:<12s} {sign}{r['distance_pct']:>9.2f}%")

    print(f"\n{'─'*100}")
    print(f"  Showing top {min(top_k, n)} lowest-priced stocks with their {period_str} low context.")
    print("=" * 100 + "\n")


def print_multi_period_results(results, period_strs, within_pct):
    """Print multi-period scan results."""
    print("\n" + "=" * 100)
    periods_header = "  ".join(f"{p}" for p in period_strs)
    print(f"  STOCKS NEAR LOW ON ALL PERIODS  |  Within {within_pct}% of low on each")
    print(f"  Periods: {periods_header}")
    print("=" * 100)

    if not results:
        print("\n  No stocks are near-low on ALL requested periods.\n")
        return

    # Header with all period low dates
    header_cols = ["#"] + [p for p in period_strs] + ["Current"]
    print(f"  {'#':<4}", end="")
    for p in period_strs:
        print(f"  {p:<16s}", end="")
    print(f"  {'Current':>10s}")
    print(f"  {'─'*4}", end="")
    for p in period_strs:
        print(f"  {'─'*16}", end="")
    print(f"  {'─'*10}")

    for i, r in enumerate(results, 1):
        print(f"  {i:<4}", end="")
        for p in period_strs:
            pp = r["per_period"][p]
            dist = pp["distance_pct"]
            sign = "" if dist < 0 else "+"
            marker = "●" if pp["is_near"] else "○"
            print(f"  {marker} {dist:>4.1f}% {pp['low_date']:<8s}", end="")
        print(f"  {r['current_price']:>10.2f}")

    print(f"\n{'─'*100}")
    print(f"  Found: {len(results)} stocks near low on ALL periods")
    print(f"  ● = within {within_pct}% of low for that period")
    print("=" * 100 + "\n")


def print_hit_today_results(results, period_str, top_k):
    """Print stocks whose period-low occurred TODAY."""
    n = len(results)
    print("\n" + "=" * 100)
    print(f"  STOCKS HIT PERIOD LOW TODAY  |  Lookback: {period_str}")
    print("=" * 100)
    print(f"  {'#':<4} {'Symbol':<22s} {'Current':>10s} {'Period Low':>12s}  "
          f"{'Low Date':<12s} {'Above Low':>10s}")
    print(f"  {'─'*4} {'─'*22} {'─'*10} {'─'*12}  {'─'*12} {'─'*10}")

    for i, r in enumerate(results[:top_k], 1):
        print(f"  {i:<4} {r['symbol']:<22s} {r['current_price']:>10.2f} {r['period_low']:>12.2f}  "
              f"{r['low_date']:<12s} {'AT LOW':>10s}")

    print(f"\n{'─'*100}")
    print(f"  Showing top {min(top_k, n)} of {n} stocks that hit their {period_str} low today.")
    print("=" * 100 + "\n")


def print_hit_this_month_results(results, period_str, top_k):
    """Print stocks whose period-low occurred THIS MONTH."""
    n = len(results)
    print("\n" + "=" * 100)
    print(f"  STOCKS HIT PERIOD LOW THIS MONTH  |  Lookback: {period_str}")
    print("=" * 100)
    print(f"  {'#':<4} {'Symbol':<22s} {'Current':>10s} {'Period Low':>12s}  "
          f"{'Low Date':<12s} {'Today':>10s}")
    print(f"  {'─'*4} {'─'*22} {'─'*10} {'─'*12}  {'─'*12} {'─'*10}")

    for i, r in enumerate(results[:top_k], 1):
        dist = (r["current_price"] - r["period_low"]) / r["period_low"] * 100
        sign = "+" if dist > 0 else ""
        print(f"  {i:<4} {r['symbol']:<22s} {r['current_price']:>10.2f} {r['period_low']:>12.2f}  "
              f"{r['low_date']:<12s} {sign}{dist:>9.1f}%")

    print(f"\n{'─'*100}")
    print(f"  Showing top {min(top_k, n)} of {n} stocks that hit their {period_str} low this month.")
    print("=" * 100 + "\n")


# ── Main ───────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Find stocks with lowest value during lookback periods"
    )

    # Period selection
    parser.add_argument("--period", type=str, default=None,
                        help="Lookback period: e.g. 1y, 3y, 5y ... (max ~6y for this data)")
    parser.add_argument("--window", type=int, default=None,
                        help="Custom window in trading days")
    parser.add_argument("--periods", type=str, default=None,
                        help="Comma-separated periods for multi-scan: e.g. 1y,3y,5y,10y")

    # Hit detection mode
    parser.add_argument("--hit", type=str, default=None,
                        choices=["today", "this-month"],
                        help="Find stocks whose period-low occurred TODAY or THIS-MONTH (overrides --mode)")

    # Legacy mode for backward compat
    parser.add_argument("--mode", type=str, default="near-low",
                        choices=["near-low", "absolute"],
                        help="[deprecated] Use --hit instead. near-low: stocks at/near period low | absolute: cheapest stocks today")
    parser.add_argument("--within-pct", type=float, default=10.0,
                        help="Within what % of the period low to consider (default: 10)")

    # Output
    parser.add_argument("--top-k", type=int, default=30,
                        help="Number of results to show (default: 30)")
    parser.add_argument("--feature", type=str, default=DEFAULT_FEATURE,
                        choices=SUPPORTED_FEATURES)
    parser.add_argument("--data_dir", type=str, default=DEFAULT_DATA_DIR)
    parser.add_argument("--cache_dir", type=str, default=DEFAULT_CACHE_DIR)
    parser.add_argument("--output", type=str, default=None,
                        help="Save results as JSON")

    args = parser.parse_args()

    if not HAS_TORCH:
        print("[ERROR] torch required: uv pip install torch")
        print("  CUDA: uv pip install torch --index-url https://download.pytorch.org/whl/cu124")
        sys.exit(1)

    # Load data
    tensor, symbols, dates = load_cached_tensor(args.feature, args.cache_dir)
    print(f"[INFO] Loaded: {tensor.shape} ({len(symbols)} stocks, {len(dates)} dates)")
    print(f"[INFO] Date range: {dates[0]} → {dates[-1]}")

    # ── Determine periods to scan ────────────────────────────────────
    period_strs = []
    windows = {}

    if args.period:
        period_strs.append(args.period)
        windows[args.period] = TRADING_DAYS[args.period]
    elif args.window:
        p = f"{args.window}d"
        period_strs.append(p)
        windows[p] = args.window

    if args.periods:
        for p in args.periods.split(","):
            p = p.strip()
            if p not in TRADING_DAYS:
                print(f"[ERROR] Unknown period '{p}'. Valid: {', '.join(TRADING_DAYS.keys())}")
                sys.exit(1)
            if p not in windows:
                period_strs.append(p)
                windows[p] = TRADING_DAYS[p]

    if not period_strs:
        print("[ERROR] Provide --period (e.g. 1y, 5y), --window (days), or --periods (1y,3y,5y)")
        sys.exit(1)

    # ── Hit detection mode (today / this-month) ────────────────────────
    if args.hit:
        print(f"[INFO] Scanning for stocks that hit their period-low {args.hit}")

        all_results = OrderedDict()
        for period_str, window in windows.items():
            print(f"[INFO]   {period_str}: {window} trading days")
            t0 = time.perf_counter()

            if args.hit == "today":
                results = find_stocks_hit_low_today(tensor, symbols, dates, window)
            else:  # this-month
                results = find_stocks_hit_low_this_month(tensor, symbols, dates, window)

            elapsed = time.perf_counter() - t0
            print(f"[INFO]   {period_str} done in {elapsed*1000:.1f}ms")

            if args.hit == "today":
                print_hit_today_results(results, period_str, args.top_k)
            else:
                print_hit_this_month_results(results, period_str, args.top_k)

            all_results[period_str] = {
                "count": len(results),
                "results": results[:args.top_k],
            }

        # Save
        if args.output:
            out = {
                "hit": args.hit,
                "feature": args.feature,
                "results": dict(all_results),
            }
            with open(args.output, "w") as f:
                json.dump(out, f, indent=2)
            print(f"[INFO] Saved to {args.output}")
        return

    # ── Legacy modes (near-low / absolute) ─────────────────────────────
    for period_str, window in windows.items():
        print(f"[INFO] Window: {window} trading days ({period_str})")

        t0 = time.perf_counter()

        if args.mode == "near-low":
            results = find_stocks_near_period_low(
                tensor, symbols, dates, window, args.within_pct
            )
            print(f"[INFO] Near-low scan done in {(time.perf_counter()-t0)*1000:.1f}ms")
            print_near_low_results(results, period_str, args.within_pct, args.top_k)
        else:  # absolute
            results = find_lowest_current_prices(tensor, symbols, dates, window)
            print(f"[INFO] Absolute price scan done in {(time.perf_counter()-t0)*1000:.1f}ms")
            results = results[:args.top_k]
            print_absolute_results(results, period_str, args.top_k)

        if args.output:
            out = {
                "mode": args.mode,
                "period": period_str,
                "window_days": window,
                "within_pct": args.within_pct if args.mode == "near-low" else None,
                "feature": args.feature,
                "results": results,
            }
            with open(args.output, "w") as f:
                json.dump(out, f, indent=2)
            print(f"[INFO] Saved to {args.output}")


if __name__ == "__main__":
    main()
