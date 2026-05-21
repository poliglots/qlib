export interface OhlcvRow {
  date: string;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
}

export interface ComparePoint {
  date: string;
  value: number;
}

export interface HeatmapItem {
  symbol: string;
  change_pct: number;
}

const BASE = "/api";

export async function fetchInstruments(market: string): Promise<string[]> {
  const res = await fetch(`${BASE}/instruments/${market}`);
  if (!res.ok) throw new Error(await res.text());
  return res.json();
}

export async function fetchOhlcv(
  symbol: string,
  start: string,
  end: string
): Promise<OhlcvRow[]> {
  const res = await fetch(
    `${BASE}/ohlcv/${encodeURIComponent(symbol)}?start=${start}&end=${end}`
  );
  if (!res.ok) throw new Error(await res.text());
  return res.json();
}

export async function fetchCompare(
  symbols: string[],
  start: string,
  end: string
): Promise<Record<string, ComparePoint[]>> {
  const res = await fetch(
    `${BASE}/compare?symbols=${symbols.join(",")}&start=${start}&end=${end}`
  );
  if (!res.ok) throw new Error(await res.text());
  return res.json();
}

export async function fetchHeatmap(
  market: string,
  days: number
): Promise<HeatmapItem[]> {
  const res = await fetch(`${BASE}/heatmap?market=${market}&days=${days}`);
  if (!res.ok) throw new Error(await res.text());
  return res.json();
}

export interface IcData {
  dates: string[];
  ic: number[];
  ric: number[];
  mean_ic: number;
  icir: number;
}

export interface SignalItem {
  symbol: string;
  score: number;
}

export interface SignalStats {
  count: number;
  mean: number;
  std: number;
  min: number;
  p25: number;
  p50: number;
  p75: number;
  max: number;
}

export interface SignalsData {
  date: string;
  available_dates: string[];
  top: SignalItem[];
  bottom: SignalItem[];
  stats: SignalStats;
}

export async function fetchIc(runId?: string): Promise<IcData> {
  const url = runId ? `${BASE}/ic?run_id=${runId}` : `${BASE}/ic`;
  const res = await fetch(url);
  if (!res.ok) throw new Error(await res.text());
  return res.json();
}

export async function fetchSignals(
  date?: string,
  topn = 20
): Promise<SignalsData> {
  const params = new URLSearchParams({ topn: String(topn) });
  if (date) params.set("date", date);
  const res = await fetch(`${BASE}/signals?${params}`);
  if (!res.ok) throw new Error(await res.text());
  return res.json();
}

// ── ML Run management ─────────────────────────────────────────────────────────

export interface MlConfig {
  model: string;
  dataset: string;
  region: string;
  path: string;
  label: string;
}

export interface MlJob {
  job_id: string;
  config: string;
  model: string;
  label: string;
  status: "running" | "success" | "failed" | "error";
  started_at: string;
  finished_at: string | null;
  return_code: number | null;
}

export interface JobLogsResponse {
  text: string;
  offset: number;
  status: MlJob["status"];
}

export async function fetchMlConfigs(): Promise<MlConfig[]> {
  const res = await fetch(`${BASE}/ml/configs`);
  if (!res.ok) throw new Error(await res.text());
  return res.json();
}

export async function triggerMlRun(
  config_path: string,
  experiment_name?: string
): Promise<{ job_id: string }> {
  const res = await fetch(`${BASE}/ml/trigger`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ config_path, experiment_name }),
  });
  if (!res.ok) throw new Error(await res.text());
  return res.json();
}

export async function fetchMlJobs(): Promise<MlJob[]> {
  const res = await fetch(`${BASE}/ml/jobs`);
  if (!res.ok) throw new Error(await res.text());
  return res.json();
}

export async function fetchJobLogs(
  job_id: string,
  offset = 0
): Promise<JobLogsResponse> {
  const res = await fetch(`${BASE}/ml/jobs/${job_id}/logs?offset=${offset}`);
  if (!res.ok) throw new Error(await res.text());
  return res.json();
}
