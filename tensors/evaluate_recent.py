#!/usr/bin/env python3
"""
Evaluate the best 10-stock portfolio for specific recent windows.

Unlike find_best_portfolio.py (which searches ALL windows), this script:
  1. Anchors to the LAST date in the data
  2. Evaluates fixed lookback windows: 1 month (~21 trading days), 6 months (~126 days), 1 year (~252 days)
  3. Returns the top 10 stocks for each window, ranked by their individual returns

Usage:
    python evaluate_recent.py                          # default: 1m, 6m, 1y
    python evaluate_recent.py --window 21              # just 1 month
    python evaluate_recent.py --window 21 63 252       # specific windows
    python evaluate_recent.py --feature adjclose       # use adjusted close
"""

import argparse
import json
import os
import sys
import time
from pathlib import Path

import numpy as np

try:
    import torch
    HAS_TORCH = True
except ImportError:
    HAS_TORCH = False

DEFAULT_DATA_DIR = os.path.expanduser("~/.qlib/stock_data/normalize/nse_1d")
DEFAULT_CACHE_DIR = os.path.expanduser("~/.qlib/stock_data/cache")
DEFAULT_FEATURE = "close"
SUPPORTED_FEATURES = ["open", "high", "low", "close", "volume", "adjclose"]

# Trading days per period (rough approximation)
TRADING_DAYS = {
    "1m": 21,
    "6m": 126,
    "1y": 252,
}


def load_cached_tensor(feature: str, cache_dir: str) -> tuple[np.ndarray, list[str], list[str]]:
    tensor_path = os.path.join(cache_dir, f"tensor_{feature}.npy")
    meta_path = os.path.join(cache_dir, f"meta_{feature}.json")

    if not os.path.exists(tensor_path):
        print("[WARN] Cache not found, building from CSVs...")
        loader = os.path.join(os.path.dirname(os.path.abspath(__file__)), "load_stock_tensors.py")
        if os.path.exists(loader):
            rc = os.system(f"uv run python3 '{loader}' --feature {feature} --cache_dir '{cache_dir}'")
            if rc != 0:
                print("[ERROR] Failed to load data")
                sys.exit(1)
        else:
            print("[ERROR] Cannot find load_stock_tensors.py")
            sys.exit(1)

    tensor = np.load(tensor_path)
    with open(meta_path) as f:
        meta = json.load(f)
    return tensor, meta["symbols"], meta["dates"]


def evaluate_recent_portfolios(
    tensor: np.ndarray,
    symbols: list[str],
    dates: list[str],
    windows: list[int],
    feature: str,
) -> list[dict]:
    """
    For each window size, anchored at the last date:
      - Compute per-stock returns over the window
      - Rank stocks by return
      - Return top 10

    tensor shape: (stocks, dates) — we use the LAST column as end date
    """
    if not HAS_TORCH:
        print("[ERROR] torch required")
        sys.exit(1)

    n_stocks, n_dates = tensor.shape
    t_gpu = torch.from_numpy(tensor).float().cuda()

    results = []

    for window in windows:
        if window >= n_dates:
            print(f"[WARN] Window {window} >= {n_dates} dates, skipping")
            continue

        t0 = time.perf_counter()

        # Start price: last window days ago
        p_end = t_gpu[:, -1]    # (stocks,) — last date
        p_start = t_gpu[:, -window]  # (stocks,) — window start

        # Return
        valid = (~torch.isnan(p_start)) & (p_start != 0) & (~torch.isnan(p_end)) & (p_end != 0)
        valid_count = int(valid.sum().item())
        returns = torch.full_like(p_end, float("nan"))
        returns[valid] = (p_end[valid] - p_start[valid]) / p_start[valid]

        # Sort and pick top 10
        valid_indices = torch.where(valid)[0]
        if len(valid_indices) == 0:
            continue
        top10_local = torch.argsort(returns[valid_indices], descending=True)[:10]
        top_syms = [symbols[int(i)] for i in valid_indices[top10_local].cpu().tolist()]
        top_rets = [float(returns[int(i)]) for i in valid_indices[top10_local].cpu().tolist()]

        # Portfolio return = mean of top 10
        top10_returns = torch.tensor(top_rets, device="cuda")
        portfolio_ret = float(top10_returns.mean())

        elapsed = time.perf_counter() - t0

        start_idx = n_dates - window
        results.append({
            "window": window,
            "start_idx": start_idx,
            "end_idx": n_dates - 1,
            "start_date": dates[start_idx],
            "end_date": dates[n_dates - 1],
            "portfolio_return": portfolio_ret,
            "n_stocks": valid_count,
            "elapsed_ms": elapsed * 1000,
            "top_10": list(zip(top_syms, top_rets)),
        })

    return results


def print_results(results: list[dict], feature: str) -> None:
    bar_width = 40

    for r in results:
        w_label = f"{r['window']}-day ({r['window'] // 21:.0f} month{'s' if r['window']//21 != 1 else ''})"
        print("\n" + "=" * 70)
        print(f"  WINDOW: {w_label}  |  {r['start_date']} → {r['end_date']}")
        print("=" * 70)
        print(f"  Feature: {feature}  |  Portfolio return: {r['portfolio_return']:+.4%}  "
              f"|  Stocks: {r['n_stocks']}  |  Computed: {r['elapsed_ms']:.1f}ms")
        print(f"\n  Top 10 stocks by individual return:")
        print(f"  {'#':<4} {'Symbol':<22s} {'Return':>10s}  {'Bar'}")
        print(f"  {'─' * 4} {'─' * 22} {'─' * 10}  {'─' * bar_width}")
        for rank, (sym, ret) in enumerate(r["top_10"], 1):
            if np.isfinite(ret):
                bar_len = min(bar_width, max(0, int(ret * 15)))
                bar = "█" * bar_len
                sign = "+" if ret > 0 else ""
                print(f"  {rank:<4} {sym:<22s} {sign}{ret:>9.2%}  {bar}")
            else:
                print(f"  {rank:<4} {sym:<22s} {'    NaN':>10s}")

    print("\n" + "=" * 70)


def main():
    parser = argparse.ArgumentParser(
        description="Evaluate best 10-stock portfolios for recent fixed windows"
    )
    parser.add_argument("windows", nargs="*", type=int,
                        help="Window sizes in trading days (default: 21 126 252)")
    parser.add_argument("--feature", type=str, default=DEFAULT_FEATURE,
                        choices=SUPPORTED_FEATURES)
    parser.add_argument("--data_dir", type=str, default=DEFAULT_DATA_DIR)
    parser.add_argument("--cache_dir", type=str, default=DEFAULT_CACHE_DIR)
    parser.add_argument("--output", type=str, default=None, help="Save results as JSON")
    parser.add_argument("--top-n", type=int, default=10, help="Number of top stocks to show")

    args = parser.parse_args()

    if not HAS_TORCH:
        print("[ERROR] torch required: uv pip install torch")
        sys.exit(1)

    # Determine windows
    if not args.windows:
        windows = [TRADING_DAYS["1m"], TRADING_DAYS["6m"], TRADING_DAYS["1y"]]
    else:
        windows = args.windows

    # Load data
    tensor, symbols, dates = load_cached_tensor(args.feature, args.cache_dir)
    print(f"[INFO] Loaded: {tensor.shape} ({len(symbols)} stocks, {len(dates)} dates)")
    print(f"[INFO] Date range: {dates[0]} → {dates[-1]}")
    print(f"[INFO] Windows to evaluate: {[f'{w}d ({w//21:.0f}m)' for w in windows]}")

    # Evaluate
    results = evaluate_recent_portfolios(tensor, symbols, dates, windows, args.feature)

    if not results:
        print("[ERROR] No valid windows computed")
        sys.exit(1)

    # Display
    print_results(results, args.feature)

    # Save
    if args.output:
        out = {
            "feature": args.feature,
            "date_range": [dates[0], dates[-1]],
            "windows": results,
        }
        with open(args.output, "w") as f:
            json.dump(out, f, indent=2)
        print(f"[INFO] Saved to {args.output}")


if __name__ == "__main__":
    main()
