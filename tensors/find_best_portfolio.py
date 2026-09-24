#!/usr/bin/env python3
"""
Find the best 10-stock portfolio over every sliding window of X days.

Uses portfolio-level vectorized computation on GPU:
  1. Compute per-stock returns for all windows simultaneously
  2. Portfolio return = mean of all valid stock returns per window
  3. Rank windows → report top-N

This is O(N × W) on GPU — no combinatorics needed.
For 2565 stocks × 1655 dates, a full scan takes ~6ms.

Usage:
    python find_best_portfolio.py --window 30 --top-k 5
    python find_best_portfolio.py --window 60 --top-k 10 --feature adjclose
    python find_best_portfolio.py --window 20 --top-k 5 --show-worst
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

# ── Constants ──────────────────────────────────────────────────────────────
DEFAULT_DATA_DIR = os.path.expanduser("~/.qlib/stock_data/normalize/nse_1d")
DEFAULT_CACHE_DIR = os.path.expanduser("~/.qlib/stock_data/cache")
DEFAULT_FEATURE = "close"
SUPPORTED_FEATURES = ["open", "high", "low", "close", "volume", "adjclose"]
MIN_STOCKS_THRESHOLD = 50  # Min stocks required in a window


# ── Data Loading ───────────────────────────────────────────────────────────
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


# ── GPU Compute ────────────────────────────────────────────────────────────
def compute_best_portfolios(tensor, symbols, dates, window, top_k, min_stocks):
    """
    For every sliding window of `window` days:
      1. Compute per-stock returns (vectorized)
      2. Portfolio return = mean of valid stock returns
      3. Rank all windows

    Returns dict with 'best' and 'worst' lists.
    """
    if not HAS_TORCH:
        print("[ERROR] torch required for GPU acceleration")
        sys.exit(1)

    n_dates = tensor.shape[1]
    if n_dates < window:
        print(f"[ERROR] Not enough dates: {n_dates} < {window}")
        sys.exit(1)

    n_windows = n_dates - window

    # Offload to GPU
    t_gpu = torch.from_numpy(tensor).float().cuda()
    print(f"[INFO] GPU: {torch.cuda.get_device_name(0)} | Tensor: {tensor.shape}")

    # ── Per-stock returns for ALL windows at once ────────────────────────
    t0 = time.perf_counter()

    # Slices — zero copies, just views into the same GPU memory
    p_start = t_gpu[:, :-window]   # (stocks, n_windows)
    p_end = t_gpu[:, window:]      # (stocks, n_windows)

    # Valid mask: both prices exist and non-zero
    mask = (~torch.isnan(p_start)) & (p_start != 0) & (~torch.isnan(p_end)) & (p_end != 0)

    # Returns tensor
    returns = torch.full_like(p_end, float("nan"))
    returns[mask] = (p_end[mask] - p_start[mask]) / p_start[mask]

    # Portfolio return per window = mean of valid stock returns
    # Mask NaN returns (stock has no data on start OR end date of window)
    valid_ret = (~torch.isnan(returns)) & (returns != 0)
    valid_count = valid_ret.sum(dim=0).float()
    # Sum only valid entries, then divide by count
    portfolio_returns = torch.where(
        valid_count >= min_stocks,
        returns.masked_fill(~valid_ret, 0).sum(dim=0) / valid_count,
        float("-inf"),
    )

    elapsed = time.perf_counter() - t0
    valid_count_total = int((portfolio_returns > float("-inf")).sum().item())
    print(f"[INFO] Computed {n_windows} windows in {elapsed*1000:.1f}ms")
    print(f"[INFO] Valid windows (≥{min_stocks} stocks): {valid_count_total}")

    # ── Find top-K and bottom-K ──────────────────────────────────────────
    clean = portfolio_returns.clone()
    clean[torch.isnan(clean)] = float("-inf")

    best_indices = clean.topk(min(top_k, n_windows), largest=True).indices.cpu().tolist()
    worst_indices = clean.topk(min(top_k, n_windows), largest=False).indices.cpu().tolist()

    # ── GPU benchmark ────────────────────────────────────────────────────
    t0 = time.perf_counter()
    for _ in range(10):
        ps = t_gpu[:, :-window]
        pe = t_gpu[:, window:]
        m = (~torch.isnan(ps)) & (ps != 0) & (~torch.isnan(pe)) & (pe != 0)
        r = torch.full_like(pe, float("nan"))
        r[m] = (pe[m] - ps[m]) / ps[m]
        vc = m.sum(dim=0).float()
        pr = torch.where(vc >= min_stocks, r.sum(dim=0) / vc, float("-inf"))
    gpu_benchmark_ms = (time.perf_counter() - t0) / 10 * 1000

    # ── Build result dicts ───────────────────────────────────────────────
    def build_result(idx):
        port_ret = float(portfolio_returns[idx])
        n_stk = int(valid_count[idx])
        wr = returns[:, idx]
        valid = ~torch.isnan(wr) & (wr != 0)
        valid_indices = torch.where(valid)[0]
        if len(valid_indices) == 0:
            top_syms, top_rets = [], []
        else:
            top10_local = torch.argsort(wr[valid_indices], descending=True)[:10]
            top_syms = [symbols[int(i)] for i in valid_indices[top10_local].cpu().tolist()]
            top_rets = [float(wr[int(i)]) for i in valid_indices[top10_local].cpu().tolist()]
        return {
            "start_idx": int(idx),
            "end_idx": int(idx + window),
            "start_date": dates[int(idx)],
            "end_date": dates[min(int(idx + window), len(dates) - 1)],
            "return": port_ret,
            "n_stocks": n_stk,
            "top_10": list(zip(top_syms, top_rets)),
        }

    return {
        "best": [build_result(i) for i in best_indices],
        "worst": [build_result(i) for i in worst_indices],
        "gpu_benchmark_ms": gpu_benchmark_ms,
    }


# ── Display ────────────────────────────────────────────────────────────────
def print_results(results, top_k, show_worst):
    bar_width = 40

    print("\n" + "=" * 70)
    print("  BEST 10-STOCK PORTFOLIO PERIODS")
    print("=" * 70)

    for i, e in enumerate(results["best"], 1):
        print(f"\n  #{i}  |  {e['start_date']} → {e['end_date']}  "
              f"({e['end_idx'] - e['start_idx']} days)")
        print(f"         Portfolio return: {e['return']:+.4%}  |  "
              f"Stocks: {e['n_stocks']}")
        print(f"         Top 10 constituents:")
        for rank, (sym, ret) in enumerate(e["top_10"], 1):
            if not np.isfinite(ret):
                print(f"           {rank:2d}. {sym:<22s}  {'    NaN':>10s}  ")
            else:
                bar_len = min(bar_width, max(0, int(ret * 20)))
                bar = "█" * bar_len
                print(f"           {rank:2d}. {sym:<22s}  {ret:+9.2%}  {bar}")

    if show_worst and results["worst"]:
        print("\n" + "=" * 70)
        print("  WORST 10-STOCK PORTFOLIO PERIODS")
        print("=" * 70)
        for i, e in enumerate(results["worst"], 1):
            print(f"\n  #{i}  |  {e['start_date']} → {e['end_date']}")
            print(f"         Portfolio return: {e['return']:+.4%}  |  "
                  f"Stocks: {e['n_stocks']}")
            for rank, (sym, ret) in enumerate(e["top_10"], 1):
                if not np.isfinite(ret):
                    print(f"           {rank:2d}. {sym:<22s}  {'    NaN':>10s}")
                else:
                    print(f"           {rank:2d}. {sym:<22s}  {ret:+9.2%}")

    print(f"\n{'=' * 70}")
    print(f"  GPU benchmark: {results['gpu_benchmark_ms']:.1f}ms per full scan")
    print("=" * 70 + "\n")


# ── Main ───────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(
        description="Find the best 10-stock portfolio over sliding windows"
    )
    parser.add_argument("--window", type=int, default=30,
                        help="Window size in days (default: 30)")
    parser.add_argument("--top-k", type=int, default=3,
                        help="Number of top portfolios to show (default: 3)")
    parser.add_argument("--show-worst", action="store_true",
                        help="Also show the worst-performing windows")
    parser.add_argument("--feature", type=str, default=DEFAULT_FEATURE,
                        choices=SUPPORTED_FEATURES)
    parser.add_argument("--data_dir", type=str, default=DEFAULT_DATA_DIR)
    parser.add_argument("--cache_dir", type=str, default=DEFAULT_CACHE_DIR)
    parser.add_argument("--min-stocks", type=int, default=MIN_STOCKS_THRESHOLD,
                        help=f"Min stocks required (default: {MIN_STOCKS_THRESHOLD})")
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

    # Compute
    results = compute_best_portfolios(
        tensor, symbols, dates,
        window=args.window,
        top_k=args.top_k,
        min_stocks=args.min_stocks,
    )

    # Display
    print_results(results, args.top_k, args.show_worst)

    # Save
    if args.output:
        out = {
            "window": args.window,
            "feature": args.feature,
            "min_stocks": args.min_stocks,
            "gpu": torch.cuda.is_available(),
            "gpu_device": torch.cuda.get_device_name(0) if torch.cuda.is_available() else None,
            "gpu_benchmark_ms": results["gpu_benchmark_ms"],
            "best": results["best"],
            "worst": results["worst"],
        }
        with open(args.output, "w") as f:
            json.dump(out, f, indent=2)
        print(f"[INFO] Saved to {args.output}")


if __name__ == "__main__":
    main()
