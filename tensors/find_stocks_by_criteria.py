#!/usr/bin/env python3
"""
Find stocks matching price range and return range for the latest window.

Anchored at the LAST date in the data, this evaluates a fixed lookback window
and returns stocks that:
  1. Have a price within [price_low, price_high] at the window start
  2. Delivered a return within [return_low, return_high]% over the window

Usage:
    # Stocks priced ₹50-100 that gave 15-25% return over the latest 1 month
    python find_stocks_by_criteria.py --price-low 50 --price-high 100 --return-low 15 --return-high 25

    # Stocks priced ₹100-500 with 10-20% return over the latest 3 months
    python find_stocks_by_criteria.py --price-low 100 --price-high 500 --return-low 10 --return-high 20 --window 63

    # No return range — just filter by price, show all returns
    python find_stocks_by_criteria.py --price-low 50 --price-high 100 --window 21

    # No return range — just filter by return, show all prices
    python find_stocks_by_criteria.py --return-low 15 --return-high 30 --window 21
"""

import argparse
import json
import os
import sys
import time
from collections import defaultdict

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

# Trading days per period (rough approximation)
# Months: 1m, 2m, 3m, 6m | Years: 1y, 2y, 3y, ... 20y
TRADING_DAYS = {
    "1m": 21,
    "2m": 42,
    "3m": 63,
    "6m": 126,
    "1y": 252,
}
for _y in range(2, 21):
    TRADING_DAYS[f"{ _y}y"] = _y * 252


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


def find_stocks_latest_window(tensor, symbols, dates, window,
                              price_low, price_high,
                              return_low, return_high, top_k, show_return_range):
    """
    Anchored at the LAST date in the data:
      1. Compute per-stock returns over the window
      2. Filter by price range at start date
      3. Filter by return range
      4. Return sorted matches

    tensor shape: (stocks, dates) — last column is the most recent date
    """
    if not HAS_TORCH:
        print("[ERROR] torch required for GPU acceleration")
        sys.exit(1)

    n_stocks, n_dates = tensor.shape
    if window >= n_dates:
        print(f"[ERROR] Window {window} >= {n_dates} dates, cannot compute")
        sys.exit(1)

    # Offload to GPU
    t_gpu = torch.from_numpy(tensor).float().cuda()
    print(f"[INFO] GPU: {torch.cuda.get_device_name(0)} | Tensor: {tensor.shape}")

    t0 = time.perf_counter()

    p_end = t_gpu[:, -1]       # (stocks,) — last date (most recent)
    p_start = t_gpu[:, -window]  # (stocks,) — window start

    # Valid mask
    valid = (~torch.isnan(p_start)) & (p_start != 0) & (~torch.isnan(p_end)) & (p_end != 0)
    valid_count = int(valid.sum().item())

    # Returns
    returns = torch.full_like(p_end, float("nan"))
    returns[valid] = (p_end[valid] - p_start[valid]) / p_start[valid]

    # End date is always the last date in the dataset
    end_date = dates[-1]
    start_date = dates[-window]

    elapsed = time.perf_counter() - t0
    print(f"[INFO] Computed return for {n_stocks} stocks in {elapsed*1000:.1f}ms")
    print(f"[INFO] Valid stocks (non-NaN price): {valid_count}")

    # ── Apply criteria ───────────────────────────────────────────────────
    price_valid = valid & (p_start >= price_low) & (p_start <= price_high)

    # Start with all valid as candidates if no return filter
    if show_return_range:
        ret_frac_low = return_low / 100.0
        ret_frac_high = return_high / 100.0
        return_valid = valid & (returns >= ret_frac_low) & (returns <= ret_frac_high)
        candidates = price_valid & return_valid
    else:
        candidates = price_valid  # Only filter by price

    # Convert to CPU
    candidates_cpu = candidates.cpu().bool()
    p_start_cpu = p_start.cpu().float()
    p_end_cpu = p_end.cpu().float()
    returns_cpu = returns.cpu().float()

    # Collect results
    results = []
    for stock_idx in range(n_stocks):
        if not candidates_cpu[stock_idx]:
            continue
        start_price = float(p_start_cpu[stock_idx])
        end_price = float(p_end_cpu[stock_idx])
        ret = float(returns_cpu[stock_idx])
        results.append({
            "symbol": symbols[stock_idx],
            "start_date": start_date,
            "end_date": end_date,
            "start_price": start_price,
            "end_price": end_price,
            "return": ret,
            "return_pct": ret * 100,
        })

    # Sort by return descending
    results.sort(key=lambda x: x["return"], reverse=True)

    # Apply top_k limit
    results = results[:top_k]

    return {
        "results": results,
        "total_available": len(results),
        "window": window,
        "start_date": start_date,
        "end_date": end_date,
        "valid_count": valid_count,
        "start_price": float(p_start_cpu[0]),  # just for metadata
    }


def print_results(results, window, price_low, price_high,
                  return_low, return_high, show_return_range, feature,
                  dates_start, dates_end, valid_count):
    n = len(results)

    # Build filter header
    filters = []
    filters.append(f"Price {price_low}-{price_high}")
    if show_return_range:
        filters.append(f"Return {return_low:.1f}-{return_high:.1f}%")
    else:
        filters.append("No return filter")

    header = f"STOCKS: {' | '.join(filters)} | Window: {window} days"

    print("\n" + "=" * 100)
    print(f"  {header}")
    print(f"  Feature: {feature}  |  Window: {dates_start} → {dates_end}")
    print("=" * 100)

    if not results:
        print("\n  No stocks matched your criteria. Try relaxing the filters.")
        print()
        return

    print(f"\n  {'#':<4} {'Symbol':<22s} {'Start Date':<12s} {'End Date':<12s}  "
          f"{'Start Price':>12s} {'End Price':>12s} {'Return':>10s}")
    print(f"  {'─' * 4} {'─' * 22} {'─' * 12} {'─' * 12} {'─' * 12} {'─' * 12} {'─' * 10}")

    for i, r in enumerate(results, 1):
        sign = "+" if r["return"] > 0 else ""
        print(f"  {i:<4} {r['symbol']:<22s} {r['start_date']:<12s} {r['end_date']:<12s}  "
              f"{r['start_price']:>12.2f} {r['end_price']:>12.2f} {sign}{r['return_pct']:>9.2f}%")

    # Summary stats
    returns = [r["return"] for r in results]
    prices_start = [r["start_price"] for r in results]
    prices_end = [r["end_price"] for r in results]
    print(f"\n{'─' * 100}")
    print(f"  Window: {dates_start} → {dates_end} ({window} trading days)")
    print(f"  Stocks found: {len(results)}  |  Valid stocks available: {valid_count}")
    print(f"  Avg return: {np.mean(returns):+.2%}  |  "
          f"Med return: {np.median(returns):+.2%}  |  "
          f"Min return: {min(returns):+.2%}  |  "
          f"Max return: {max(returns):+.2%}")
    print(f"  Avg start price: {np.mean(prices_start):.2f}  |  Avg end price: {np.mean(prices_end):.2f}")
    print("=" * 100)


def main():
    parser = argparse.ArgumentParser(
        description="Find stocks by price and return range for the latest window"
    )

    # Price range
    parser.add_argument("--price-low", type=float, default=None,
                        help="Minimum stock price at window start")
    parser.add_argument("--price-high", type=float, default=None,
                        help="Maximum stock price at window start")

    # Return range
    parser.add_argument("--return-low", type=float, default=None,
                        help="Minimum return percentage, e.g., 10 for 10 pct")
    parser.add_argument("--return-high", type=float, default=None,
                        help="Maximum return percentage, e.g., 25 for 25 pct")

    # Window
    parser.add_argument("--window", type=int, default=None,
                        help="Window size in trading days (overrides --period)")
    parser.add_argument("--period", type=str, default=None,
                        choices=TRADING_DAYS.keys(),
                        help="Preset period: 1m, 2m, 3m, 6m, 1y, 2y, 3y ... 20y (overrides --window)")

    parser.add_argument("--top-k", type=int, default=30,
                        help="Number of top results to show, default 30")
    parser.add_argument("--feature", type=str, default=DEFAULT_FEATURE,
                        choices=SUPPORTED_FEATURES)
    parser.add_argument("--data_dir", type=str, default=DEFAULT_DATA_DIR)
    parser.add_argument("--cache_dir", type=str, default=DEFAULT_CACHE_DIR)
    parser.add_argument("--output", type=str, default=None,
                        help="Save results as JSON")

    args = parser.parse_args()

    # Validate: must have at least one filter
    has_price = args.price_low is not None and args.price_high is not None
    has_return = args.return_low is not None and args.return_high is not None

    if not has_price and not has_return:
        print("[ERROR] Provide at least --price-low/--price-high or --return-low/--return-high")
        sys.exit(1)

    if has_price and (args.price_low >= args.price_high):
        print(f"[ERROR] --price-low ({args.price_low}) must be < --price-high ({args.price_high})")
        sys.exit(1)

    if has_return and (args.return_low >= args.return_high):
        print(f"[ERROR] --return-low ({args.return_low}) must be < --return-high ({args.return_high})")
        sys.exit(1)

    if not HAS_TORCH:
        print("[ERROR] torch required: uv pip install torch")
        sys.exit(1)

    # Determine window
    if args.period:
        window = TRADING_DAYS[args.period]
    elif args.window:
        window = args.window
    else:
        window = TRADING_DAYS["1m"]  # default

    show_return_range = has_return

    # Load data
    tensor, symbols, dates = load_cached_tensor(args.feature, args.cache_dir)
    print(f"[INFO] Loaded: {tensor.shape} ({len(symbols)} stocks, {len(dates)} dates)")
    print(f"[INFO] Date range: {dates[0]} → {dates[-1]}")
    # Compute human-friendly label
    if window <= 63:
        label = f"{window//21:.0f} month{'s' if window//21 != 1 else ''}"
    elif window <= 378:
        label = f"{window//252:.0f} year{'s' if window//252 != 1 else ''}"
    else:
        label = f"{window//252:.0f} year{'s' if window//252 != 1 else ''} ({window//30:.0f} months)"
    print(f"[INFO] Window: {window} days ({label})")

    # Compute
    results = find_stocks_latest_window(
        tensor, symbols, dates, window,
        price_low=args.price_low,
        price_high=args.price_high,
        return_low=args.return_low,
        return_high=args.return_high,
        top_k=args.top_k,
        show_return_range=show_return_range,
    )

    # Display
    print_results(
        results["results"], window,
        price_low=args.price_low,
        price_high=args.price_high,
        return_low=args.return_low,
        return_high=args.return_high,
        show_return_range=show_return_range,
        feature=args.feature,
        dates_start=results["start_date"],
        dates_end=results["end_date"],
        valid_count=results["valid_count"],
    )

    # Save
    if args.output:
        out = {
            "criteria": {
                "price_range": [args.price_low, args.price_high] if has_price else None,
                "return_range_pct": [args.return_low, args.return_high] if has_return else None,
                "window_days": window,
                "feature": args.feature,
            },
            "window_info": {
                "start_date": results["start_date"],
                "end_date": results["end_date"],
                "valid_count": results["valid_count"],
            },
            "total_available": results["total_available"],
            "results": results["results"],
        }
        with open(args.output, "w") as f:
            json.dump(out, f, indent=2)
        print(f"[INFO] Saved to {args.output}")


if __name__ == "__main__":
    main()
