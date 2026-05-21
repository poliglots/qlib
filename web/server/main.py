from __future__ import annotations

import asyncio
import os
import pickle
import re
import time
import uuid
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

QLIB_DATA_DIR = os.environ.get("QLIB_DATA_DIR", str(Path.home() / ".qlib/qlib_data/nse_data"))
MLRUNS_DIR = os.environ.get(
    "MLRUNS_DIR",
    str(Path(__file__).resolve().parents[2] / "mlruns"),
)
VALID_MARKETS = {"nifty50", "nifty500", "all"}

_BENCHMARKS_DIR = Path(__file__).resolve().parents[2] / "examples" / "benchmarks"
_LOGS_DIR = Path(__file__).resolve().parent / "job_logs"
_QRUN = str(Path(__file__).resolve().parents[2] / ".venv" / "bin" / "qrun")
_JOBS_FILE = Path(__file__).resolve().parent / "jobs.json"
_LOGS_DIR.mkdir(exist_ok=True)


def _load_jobs() -> dict[str, dict]:
    if _JOBS_FILE.exists():
        import json
        try:
            jobs = json.loads(_JOBS_FILE.read_text())
            # Any job still marked running didn't finish cleanly — mark failed
            for j in jobs.values():
                if j["status"] == "running":
                    j["status"] = "failed"
            return jobs
        except Exception:
            return {}
    return {}


def _save_jobs() -> None:
    import json
    _JOBS_FILE.write_text(json.dumps(_jobs, indent=2))


# Job store: persisted to disk across restarts
_jobs: dict[str, dict] = _load_jobs()


@asynccontextmanager
async def lifespan(app: FastAPI):
    import qlib

    qlib.init(provider_uri=QLIB_DATA_DIR, expression_cache=None, dataset_cache=None)
    yield


app = FastAPI(title="NSE Stock Dashboard API", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)


def _get_df(
    market: str,
    symbols: list[str] | None,
    fields: list[str],
    start: str,
    end: str,
) -> pd.DataFrame:
    from qlib.data import D

    if symbols:
        instruments = symbols
    else:
        instruments = D.instruments(market)

    df = D.features(
        instruments,
        fields,
        start_time=start,
        end_time=end,
        freq="day",
    )
    return df


@app.get("/api/instruments/{market}")
def get_instruments(market: str) -> list[str]:
    if market not in VALID_MARKETS:
        raise HTTPException(400, f"market must be one of {sorted(VALID_MARKETS)}")
    from qlib.data import D

    inst = D.list_instruments(D.instruments(market), as_list=True)
    return sorted(inst)


@app.get("/api/ohlcv/{symbol}")
def get_ohlcv(
    symbol: str,
    start: str = Query("2020-01-01"),
    end: str = Query(str(pd.Timestamp.today().date())),
):
    fields = ["$open", "$high", "$low", "$close", "$volume"]
    try:
        df = _get_df(market="all", symbols=[symbol], fields=fields, start=start, end=end)
    except Exception as exc:
        raise HTTPException(404, f"No data for {symbol}: {exc}") from exc

    if df.empty:
        raise HTTPException(404, f"No data for {symbol} in [{start}, {end}]")

    df = df.xs(symbol, level="instrument")
    df.index = df.index.strftime("%Y-%m-%d")
    df.columns = ["open", "high", "low", "close", "volume"]
    df = df.dropna(subset=["close"])

    rows = [
        {"date": date, **{col: round(float(val), 4) for col, val in row.items()}}
        for date, row in df.iterrows()
    ]
    return rows


@app.get("/api/compare")
def get_compare(
    symbols: str = Query(..., description="Comma-separated symbols"),
    start: str = Query("2023-01-01"),
    end: str = Query(str(pd.Timestamp.today().date())),
):
    symbol_list = [s.strip().upper() for s in symbols.split(",") if s.strip()]
    if not symbol_list:
        raise HTTPException(400, "symbols must not be empty")

    fields = ["$close"]
    try:
        df = _get_df(market="all", symbols=symbol_list, fields=fields, start=start, end=end)
    except Exception as exc:
        raise HTTPException(500, str(exc)) from exc

    result: dict[str, list] = {}
    for sym in symbol_list:
        try:
            s = df.xs(sym, level="instrument")["$close"].dropna()
        except KeyError:
            continue
        if s.empty:
            continue
        base = s.iloc[0]
        normalised = (s / base * 100).round(4)
        result[sym] = [
            {"date": ts.strftime("%Y-%m-%d"), "value": v}
            for ts, v in normalised.items()
        ]
    return result


@app.get("/api/heatmap")
def get_heatmap(
    market: str = Query("nifty50"),
    days: int = Query(5, ge=1, le=252),
):
    if market not in VALID_MARKETS:
        raise HTTPException(400, f"market must be one of {sorted(VALID_MARKETS)}")

    # Need enough history to cover `days` trading days — fetch 2× as buffer
    end = pd.Timestamp.today().normalize()
    start = end - pd.offsets.BDay(days * 2 + 10)

    fields = ["$close"]
    try:
        df = _get_df(
            market=market,
            symbols=None,
            fields=fields,
            start=str(start.date()),
            end=str(end.date()),
        )
    except Exception as exc:
        raise HTTPException(500, str(exc)) from exc

    result = []
    for sym, group in df.groupby(level="instrument"):
        closes = group["$close"].dropna()
        if len(closes) < 2:
            continue
        # Use last `days` trading sessions
        recent = closes.iloc[-(days + 1):]
        if len(recent) < 2:
            continue
        change_pct = round(float((recent.iloc[-1] / recent.iloc[0] - 1) * 100), 4)
        result.append({"symbol": sym, "change_pct": change_pct})

    result.sort(key=lambda x: x["change_pct"], reverse=True)
    return result


# ── Experiment / signal endpoints ────────────────────────────────────────────


def _latest_run_dir() -> Path:
    """Return the artifacts dir of the most-recently-finished MLflow run that has ic.pkl."""
    mlruns = Path(MLRUNS_DIR)
    artifact_dirs = sorted(
        mlruns.glob("*/*/artifacts"),
        key=lambda p: p.parent.stat().st_mtime,
        reverse=True,
    )
    if not artifact_dirs:
        raise FileNotFoundError("No MLflow runs found in mlruns/")
    for art in artifact_dirs:
        if (art / "sig_analysis" / "ic.pkl").exists():
            return art
    raise FileNotFoundError("No MLflow runs found in mlruns/")


def _load_pkl(path: Path):
    with open(path, "rb") as f:
        return pickle.load(f)


@app.get("/api/runs")
def list_runs():
    """List completed MLflow runs with their IC metrics."""
    mlruns = Path(MLRUNS_DIR)
    result = []
    for meta in sorted(mlruns.glob("*/*/meta.yaml"), key=lambda p: p.stat().st_mtime, reverse=True):
        run_dir = meta.parent
        metrics: dict = {}
        for mfile in (run_dir / "metrics").glob("*"):
            try:
                lines = mfile.read_text().strip().splitlines()
                val = float(lines[-1].split()[1])
                metrics[mfile.name] = round(val, 6)
            except Exception:
                pass
        if not (run_dir / "artifacts" / "pred.pkl").exists():
            continue
        result.append({
            "run_id": run_dir.name,
            "experiment_id": run_dir.parent.name,
            "name": (run_dir / "tags" / "mlflow.runName").read_text().strip()
                if (run_dir / "tags" / "mlflow.runName").exists() else run_dir.name[:8],
            "metrics": metrics,
        })
    return result


@app.get("/api/ic")
def get_ic(run_id: str = Query(None)):
    """Return daily IC and Rank IC for the given (or latest) run."""
    try:
        art = _latest_run_dir() if run_id is None else _find_run_artifacts(run_id)
    except FileNotFoundError as exc:
        raise HTTPException(404, str(exc)) from exc

    ic_path = art / "sig_analysis" / "ic.pkl"
    ric_path = art / "sig_analysis" / "ric.pkl"
    if not ic_path.exists():
        raise HTTPException(404, "ic.pkl not found — run SigAnaRecord first")

    ic: pd.Series = _load_pkl(ic_path)
    ric: pd.Series = _load_pkl(ric_path) if ric_path.exists() else pd.Series(dtype=float)

    dates = ic.index.strftime("%Y-%m-%d").tolist()
    return {
        "dates": dates,
        "ic": [round(float(v), 6) for v in ic.values],
        "ric": [round(float(v), 6) for v in ric.reindex(ic.index).values] if not ric.empty else [],
        "mean_ic": round(float(ic.mean()), 6),
        "icir": round(float(ic.mean() / ic.std()), 6) if ic.std() else 0,
    }


@app.get("/api/signals")
def get_signals(run_id: str = Query(None), date: str = Query(None), topn: int = Query(20)):
    """Return top-N and bottom-N signal scores for a given date (default: latest)."""
    try:
        art = _latest_run_dir() if run_id is None else _find_run_artifacts(run_id)
    except FileNotFoundError as exc:
        raise HTTPException(404, str(exc)) from exc

    pred_path = art / "pred.pkl"
    if not pred_path.exists():
        raise HTTPException(404, "pred.pkl not found")

    pred: pd.DataFrame = _load_pkl(pred_path)
    dates_avail = pred.index.get_level_values("datetime").unique()

    if date is None:
        ts = dates_avail.max()
    else:
        ts = pd.Timestamp(date)
        if ts not in dates_avail:
            # snap to nearest available date
            ts = dates_avail[dates_avail <= ts].max() if any(dates_avail <= ts) else dates_avail.min()

    day_df = pred.xs(ts, level="datetime")["score"].sort_values(ascending=False)
    top = [{"symbol": sym, "score": round(float(v), 6)} for sym, v in day_df.head(topn).items()]
    bottom = [{"symbol": sym, "score": round(float(v), 6)} for sym, v in day_df.tail(topn).items()]
    available_dates = dates_avail.strftime("%Y-%m-%d").tolist()

    clean = day_df.dropna()
    stats = {
        "count": int(len(clean)),
        "mean": round(float(clean.mean()), 6),
        "std": round(float(clean.std()), 6),
        "min": round(float(clean.min()), 6),
        "p25": round(float(clean.quantile(0.25)), 6),
        "p50": round(float(clean.quantile(0.50)), 6),
        "p75": round(float(clean.quantile(0.75)), 6),
        "max": round(float(clean.max()), 6),
    }

    return {
        "date": ts.strftime("%Y-%m-%d"),
        "available_dates": available_dates,
        "top": top,
        "bottom": bottom,
        "stats": stats,
    }


def _find_run_artifacts(run_id: str) -> Path:
    mlruns = Path(MLRUNS_DIR)
    matches = list(mlruns.glob(f"*/{run_id}/artifacts"))
    if not matches:
        raise FileNotFoundError(f"Run {run_id} not found")
    return matches[0]


# ── ML Run management ─────────────────────────────────────────────────────────

_CONFIG_RE = re.compile(
    r"workflow_config_(?P<model>[^_]+(?:_[^_]+)*)_(?P<dataset>Alpha\d+)(?:_(?P<region>[^.]+))?\.yaml",
    re.IGNORECASE,
)


@app.get("/api/ml/configs")
def list_ml_configs():
    """Return available workflow YAML configs grouped by model."""
    results = []
    for yaml_path in sorted(_BENCHMARKS_DIR.rglob("*.yaml")):
        m = _CONFIG_RE.match(yaml_path.name)
        if not m:
            continue
        results.append({
            "model": yaml_path.parent.name,
            "dataset": m.group("dataset"),
            "region": m.group("region") or "CSI",
            "path": str(yaml_path),
            "label": f"{yaml_path.parent.name} / {m.group('dataset')}"
            + (f" / {m.group('region')}" if m.group("region") else ""),
        })
    return results


class TriggerRequest(BaseModel):
    config_path: str
    experiment_name: Optional[str] = None


@app.post("/api/ml/trigger")
async def trigger_ml_run(req: TriggerRequest):
    """Launch a qrun subprocess for the given config and return a job id."""
    config = Path(req.config_path)
    if not config.exists():
        raise HTTPException(400, f"Config not found: {req.config_path}")

    job_id = uuid.uuid4().hex[:12]
    log_path = _LOGS_DIR / f"{job_id}.log"

    env = {**os.environ, "MLRUNS_DIR": MLRUNS_DIR, "QLIB_DATA_DIR": QLIB_DATA_DIR}

    cmd = [_QRUN, str(config)]
    if req.experiment_name:
        cmd += ["--experiment_name", req.experiment_name]

    _jobs[job_id] = {
        "job_id": job_id,
        "config": str(config),
        "model": config.parent.name,
        "label": config.stem,
        "status": "running",
        "started_at": datetime.utcnow().isoformat(),
        "finished_at": None,
        "log_path": str(log_path),
        "return_code": None,
    }
    _save_jobs()

    async def _run():
        try:
            with open(log_path, "w") as lf:
                proc = await asyncio.create_subprocess_exec(
                    *cmd,
                    stdout=lf,
                    stderr=asyncio.subprocess.STDOUT,
                    env=env,
                    cwd=str(config.parent),
                )
                rc = await proc.wait()
            _jobs[job_id]["status"] = "success" if rc == 0 else "failed"
            _jobs[job_id]["return_code"] = rc
        except Exception as exc:
            with open(log_path, "a") as lf:
                lf.write(f"\n[ERROR] {exc}\n")
            _jobs[job_id]["status"] = "error"
        finally:
            _jobs[job_id]["finished_at"] = datetime.utcnow().isoformat()
            _save_jobs()

    asyncio.create_task(_run())
    return {"job_id": job_id}


@app.get("/api/ml/jobs")
def list_ml_jobs():
    """Return all tracked ML jobs (running + completed)."""
    return list(reversed(list(_jobs.values())))


@app.get("/api/ml/jobs/{job_id}/logs")
def get_job_logs(job_id: str, offset: int = Query(0)):
    """Return log text for a job starting at byte offset (for incremental polling)."""
    if job_id not in _jobs:
        raise HTTPException(404, f"Job {job_id} not found")
    log_path = Path(_jobs[job_id]["log_path"])
    if not log_path.exists():
        return {"text": "", "offset": 0, "status": _jobs[job_id]["status"]}
    content = log_path.read_bytes()
    chunk = content[offset:]
    return {
        "text": chunk.decode("utf-8", errors="replace"),
        "offset": len(content),
        "status": _jobs[job_id]["status"],
    }
