#!/usr/bin/env python3
"""
Load all NSE stock data from normalized CSVs into a single M×N tensor.

M = number of stocks (rows), N = number of trading dates (columns).
Each column represents one date; each row contains price values for one
stock across all dates.

Speed features:
  1. Parallel CSV loading (concurrent.futures)
  2. Binary cache (.npy/.npz) — skips all CSV I/O on subsequent runs
  3. Optional GPU offload (PyTorch)

Usage:
    python load_stock_tensors.py [--data_dir PATH] [--feature close] [--window N]

Examples:
    python load_stock_tensors.py                     # load + cache
    python load_stock_tensors.py --window 30         # last 30 days
    python load_stock_tensors.py --feature adjclose  # different column
    python load_stock_tensors.py --gpu               # move to GPU
"""

import argparse
import json
import os
import sys
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
from typing import Optional

import numpy as np

# ---------------------------------------------------------------------------
# Torch is optional — used for GPU offload & GPU-side computation
# ---------------------------------------------------------------------------
try:
    import torch
    HAS_TORCH = True
except ImportError:
    HAS_TORCH = False

DEFAULT_DATA_DIR = os.path.expanduser("~/.qlib/stock_data/normalize/nse_1d")
DEFAULT_CACHE_DIR = os.path.expanduser("~/.qlib/stock_data/cache")
DEFAULT_FEATURE = "close"
SUPPORTED_FEATURES = ["open", "high", "low", "close", "volume", "adjclose"]


# ===================================================================
# Worker function — runs in a separate process, loads a single CSV
# ===================================================================
def _load_one_csv(args: tuple) -> Optional[tuple]:
    """Load a single stock CSV and return (symbol, {date_str: value}).

    Uses the stdlib csv module so it works in subprocesses without
    requiring pandas in the child process.
    """
    csv_file, feature = args
    try:
        date_map: dict[str, float] = {}
        with open(csv_file, newline="") as f:
            reader = __import__("csv").DictReader(f)
            for row in reader:
                date_str = row["date"]
                val = row.get(feature)
                if val is not None and val != "":
                    date_map[date_str] = float(val)
        if not date_map:
            return None
        return (os.path.splitext(os.path.basename(csv_file))[0], date_map)
    except Exception:
        return None


# ===================================================================
# Core: build the full (M, N) array
# ===================================================================
def _build_tensor(data_dir: str, feature: str, workers: int = 8) -> tuple[np.ndarray, list[str], list[str]]:
    """
    Load every CSV, align by date, return (tensor, symbols, dates).
    """
    data_path = Path(data_dir)
    csv_files = sorted(data_path.glob("*.csv"))

    if not csv_files:
        print(f"[ERROR] No CSV files in {data_dir}")
        sys.exit(1)

    print(f"[INFO] Found {len(csv_files)} stock CSV files")

    # --- Parallel loading ---
    t0 = time.perf_counter()
    stock_symbols: list[str] = []
    stock_features: dict[str, dict[str, float]] = {}

    with ProcessPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(_load_one_csv, (str(f), feature)): f for f in csv_files}
        for fut in as_completed(futures):
            result = fut.result()
            if result is None:
                continue
            sym, date_map = result
            stock_symbols.append(sym)
            stock_features[sym] = date_map

    load_time = time.perf_counter() - t0
    print(f"[INFO] CSV loading done ({workers} workers): {load_time:.1f}s")

    if not stock_symbols:
        print(f"[ERROR] No stocks had feature '{feature}'")
        sys.exit(1)

    # --- Align dates ---
    all_dates = sorted({d for dm in stock_features.values() for d in dm})
    print(f"[INFO] Date range: {all_dates[0]} → {all_dates[-1]} ({len(all_dates)} dates)")

    # --- Build array ---
    n_stocks = len(stock_symbols)
    n_dates = len(all_dates)
    tensor = np.full((n_stocks, n_dates), np.nan, dtype=np.float32)

    for i, sym in enumerate(stock_symbols):
        for j, date in enumerate(all_dates):
            v = stock_features[sym].get(date)
            if v is not None:
                tensor[i, j] = v

    filled = np.count_nonzero(~np.isnan(tensor))
    total = n_stocks * n_dates
    print(f"[INFO] Tensor shape: {tensor.shape}, filled: {filled}/{total} "
          f"({100 * filled / total:.1f}%)")

    return tensor, stock_symbols, all_dates


# ===================================================================
# Cache management
# ===================================================================
def _cache_path(cache_dir: str, feature: str) -> tuple[str, str, str]:
    """Return (tensor_cache, meta_cache, hash_cache) paths."""
    os.makedirs(cache_dir, exist_ok=True)
    safe = feature.replace("/", "_").replace("..", "_")
    return (
        os.path.join(cache_dir, f"tensor_{safe}.npy"),
        os.path.join(cache_dir, f"meta_{safe}.json"),
        os.path.join(cache_dir, f"hash_{safe}.txt"),
    )


def _cache_needs_rebuild(cache_dir: str, feature: str, data_dir: str) -> bool:
    """Decide whether to re-read CSVs by comparing file hashes."""
    _, _, hash_path = _cache_path(cache_dir, feature)
    if not os.path.exists(hash_path):
        return True

    # Compute a combined hash of all CSV files' modification times
    data_path = Path(data_dir)
    csv_files = sorted(data_path.glob("*.csv"))
    current = sum(
        os.path.getmtime(f) for f in csv_files
    )
    try:
        with open(hash_path) as f:
            stored = float(f.read().strip())
    except (ValueError, OSError):
        return True

    return abs(current - stored) > 1e-6


def _save_cache(tensor: np.ndarray, symbols: list[str], dates: list[str],
                feature: str, cache_dir: str, data_dir: str) -> None:
    """Save tensor + metadata to .npy + .json, and store a rebuild hash."""
    tensor_path, meta_path, hash_path = _cache_path(cache_dir, feature)

    np.save(tensor_path, tensor)
    with open(meta_path, "w") as f:
        json.dump({"symbols": symbols, "dates": dates, "feature": feature}, f)

    current = sum(os.path.getmtime(f) for f in Path(data_dir).glob("*.csv"))
    with open(hash_path, "w") as f:
        f.write(str(current))

    print(f"[INFO] Cached to {tensor_path}")


def _load_cache(feature: str, cache_dir: str) -> tuple[np.ndarray, list[str], list[str]]:
    tensor_path, meta_path, _ = _cache_path(cache_dir, feature)
    tensor = np.load(tensor_path)
    with open(meta_path) as f:
        meta = json.load(f)
    print(f"[INFO] Loaded from cache: {tensor.shape}")
    return tensor, meta["symbols"], meta["dates"]


# ===================================================================
# GPU helpers
# ===================================================================
def _to_gpu(tensor: np.ndarray) -> torch.Tensor:
    """Move a numpy array to CUDA if available."""
    if not HAS_TORCH:
        print("[WARN] torch not installed — staying on CPU")
        return torch.from_numpy(tensor)

    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    t = torch.from_numpy(tensor).to(device)
    print(f"[INFO] Moved to {device} ({torch.cuda.get_device_name(0) if device.type == 'cuda' else 'CPU'})")
    return t


# ===================================================================
# GPU-side computation helpers
# ===================================================================
def compute_returns(tensor_gpu: torch.Tensor) -> torch.Tensor:
    """
    Compute daily returns: R_t = (P_t - P_{t-1}) / P_{t-1}.
    Input: (stocks, dates), Output: (stocks, dates-1) — NaN removed.
    """
    p0 = tensor_gpu[:, :-1]
    p1 = tensor_gpu[:, 1:]
    mask = ~torch.isnan(p0) & (p0 != 0)
    ret = torch.full_like(p1, float("nan"))
    ret[mask] = (p1[mask] - p0[mask]) / p0[mask]
    return ret


def compute_mean_std(tensor_gpu: torch.Tensor, dim: int = 1) -> tuple[torch.Tensor, torch.Tensor]:
    """
    Compute mean and std along `dim`, ignoring NaN.
    dim=1 → per-stock statistics; dim=0 → per-date statistics.
    """
    mask = ~torch.isnan(tensor_gpu)
    count = mask.sum(dim=dim, keepdim=True)
    mean = torch.where(count > 0, tensor_gpu.masked_fill(~mask, 0).sum(dim=dim, keepdim=True) / count, float("nan"))
    var = torch.where(
        count > 1,
        tensor_gpu.masked_fill(~mask, 0).sum(dim=dim, keepdim=True) ** 2 / (count * (count - 1)),
        0,
    ) + tensor_gpu.masked_fill(~mask, 0).square().sum(dim=dim, keepdim=True) / (count - 1)
    std = torch.sqrt(torch.clamp(var, min=0))
    std = torch.where(count > 0, std, float("nan"))
    return mean.squeeze(dim), std.squeeze(dim)


def zscore_normalize(tensor_gpu: torch.Tensor, dim: int = 1) -> torch.Tensor:
    """Z-score normalize along `dim` (default: per-stock)."""
    mean, std = compute_mean_std(tensor_gpu, dim=dim)
    # Broadcast mean/std back
    expand_size = list(tensor_gpu.shape)
    expand_size[dim] = 1
    mean = mean.view(expand_size)
    std = std.view(expand_size)
    return torch.where(std != 0, (tensor_gpu - mean) / std, float("nan"))


def correlation_matrix(tensor_gpu: torch.Tensor) -> torch.Tensor:
    """
    Compute cross-sectional correlation matrix (stocks × stocks).
    Uses last 60 dates or all available.
    """
    n_dates = tensor_gpu.shape[1]
    window = min(60, n_dates)
    slice_t = tensor_gpu[:, -window:]

    # Drop stocks with all-NaN in window
    valid = torch.any(~torch.isnan(slice_t), dim=1)
    s = slice_t[valid]

    n_stocks = s.shape[0]
    if n_stocks < 2:
        return torch.tensor([], device=s.device)

    # Mean along dates
    mask = ~torch.isnan(s)
    count = mask.sum(dim=1, keepdim=True)
    mean = torch.where(count > 0, s.masked_fill(~mask, 0).sum(dim=1, keepdim=True) / count, 0)

    # Center
    centered = s - mean  # (n, window)

    # Covariance: (n × n) stocks×stocks = centered @ centered.T / (window-1)
    cov = centered @ centered.T / (window - 1)

    # Std from diagonal
    std = torch.sqrt(torch.clamp(torch.diag(cov), min=0))
    denom = torch.ger(std, std)
    corr = torch.where(denom != 0, cov / denom, 0)

    # Zero out invalid stocks
    inv_mask = torch.zeros(n_stocks, n_stocks, device=s.device)
    valid_idx = torch.where(valid)[0]
    inv_mask[torch.arange(n_stocks), torch.arange(n_stocks)] = 1.0
    # Set invalid rows/cols to NaN
    for i in range(n_stocks):
        if not valid[i]:
            corr[i, :] = float("nan")
            corr[:, i] = float("nan")

    return corr


# ===================================================================
# Main
# ===================================================================
def main():
    parser = argparse.ArgumentParser(
        description="Load NSE stock CSVs → M×N tensor with GPU compute support"
    )
    parser.add_argument("--data_dir", type=str, default=DEFAULT_DATA_DIR,
                        help="Directory containing stock CSVs")
    parser.add_argument("--feature", type=str, default=DEFAULT_FEATURE,
                        choices=SUPPORTED_FEATURES,
                        help="Feature column to use")
    parser.add_argument("--cache_dir", type=str, default=DEFAULT_CACHE_DIR,
                        help="Directory for binary cache")
    parser.add_argument("--no_cache", action="store_true",
                        help="Force reload from CSVs (ignore cache)")
    parser.add_argument("--window", type=int, default=None,
                        help="Keep last N dates only")
    parser.add_argument("--save", type=str, default=None,
                        help="Save output tensor as .npy")
    parser.add_argument("--save_meta", type=str, default=None,
                        help="Save stock/dates metadata as .json")
    parser.add_argument("--json", type=str, default=None,
                        help="Save tensor+metadata as .npz")
    parser.add_argument("--gpu", action="store_true",
                        help="Move tensor to GPU")
    parser.add_argument("--compute", type=str, choices=[
        "returns", "mean_std", "zscore", "correlation"
    ], default=None,
                        help="Run a GPU computation on the tensor")
    default_workers = max(1, os.cpu_count() // 2)
    parser.add_argument("--workers", type=int, default=default_workers,
                        help=f"Parallel CSV workers (default: {default_workers})")

    args = parser.parse_args()

    workers = args.workers

    # --- Load or rebuild tensor ---
    if args.no_cache or not _cache_needs_rebuild(args.cache_dir, args.feature, args.data_dir):
        tensor, symbols, dates = _load_cache(args.feature, args.cache_dir)
    else:
        tensor, symbols, dates = _build_tensor(args.data_dir, args.feature, workers)
        _save_cache(tensor, symbols, dates, args.feature, args.cache_dir, args.data_dir)

    # --- Apply window ---
    if args.window is not None:
        if args.window > tensor.shape[1]:
            print(f"[WARN] Window {args.window} > {tensor.shape[1]}, keeping all")
        else:
            idx = max(0, tensor.shape[1] - args.window)
            tensor = tensor[:, idx:]
            dates = dates[idx:]
            print(f"[INFO] Window → last {args.window} dates")

    print(f"\n[RESULT] Tensor: {tensor.shape} | Feature: {args.feature} | "
          f"Dates: {dates[0]} → {dates[-1]}")

    # --- Optional GPU offload ---
    tensor_gpu = None
    if args.gpu:
        if not HAS_TORCH:
            print("[ERROR] --gpu requires torch: pip install torch")
            sys.exit(1)
        tensor_gpu = _to_gpu(tensor)
        # Also move metadata
        symbols_gpu = symbols  # symbols stay on CPU (strings)
        dates_gpu = dates

    # --- GPU-side computations ---
    if args.compute and tensor_gpu is not None:
        t0 = time.perf_counter()
        if args.compute == "returns":
            result = compute_returns(tensor_gpu)
            print(f"[COMPUTE] Daily returns: {result.shape}")
        elif args.compute == "mean_std":
            mean, std = compute_mean_std(tensor_gpu)
            print(f"[COMPUTE] Mean: {mean.shape}, Std: {std.shape}")
            print(f"    Global mean: {mean.mean():.4f}, Global std: {std.mean():.4f}")
            return
        elif args.compute == "zscore":
            result = zscore_normalize(tensor_gpu)
            print(f"[COMPUTE] Z-score normalized: {result.shape}")
        elif args.compute == "correlation":
            result = correlation_matrix(tensor_gpu)
            print(f"[COMPUTE] Correlation matrix: {result.shape}")
        elapsed = time.perf_counter() - t0
        print(f"    Took {elapsed*1000:.1f}ms on GPU")
        return

    if args.compute and not args.gpu:
        print("[WARN] --compute requires --gpu; tensor is still on CPU")

    # --- Save ---
    if args.save:
        np.save(args.save, tensor)
        print(f"[INFO] Saved to {args.save}")
    if args.save_meta:
        with open(args.save_meta, "w") as f:
            json.dump({"symbols": symbols, "dates": dates, "feature": args.feature}, f)
        print(f"[INFO] Saved meta to {args.save_meta}")
    if args.json:
        np.savez_compressed(
            args.json,
            tensor=tensor,
            symbols=np.array(symbols),
            dates=np.array(dates),
            feature=np.array([args.feature]),
        )
        print(f"[INFO] Saved to {args.json}")


if __name__ == "__main__":
    main()
