import { useEffect, useRef, useState } from "react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import {
  fetchMlConfigs,
  fetchMlJobs,
  fetchJobLogs,
  triggerMlRun,
  MlConfig,
  MlJob,
} from "../api/client";

// ── Status badge ───────────────────────────────────────────────────────────────

const STATUS_STYLES: Record<MlJob["status"], string> = {
  running: "bg-blue-500/20 text-blue-300 border border-blue-500/40",
  success: "bg-green-500/20 text-green-300 border border-green-500/40",
  failed: "bg-red-500/20 text-red-300 border border-red-500/40",
  error: "bg-orange-500/20 text-orange-300 border border-orange-500/40",
};

function StatusBadge({ status }: { status: MlJob["status"] }) {
  return (
    <span className={`px-2 py-0.5 rounded text-xs font-mono ${STATUS_STYLES[status]}`}>
      {status === "running" && (
        <span className="inline-block w-1.5 h-1.5 rounded-full bg-blue-400 animate-pulse mr-1" />
      )}
      {status}
    </span>
  );
}

// ── Config selector panel ──────────────────────────────────────────────────────

function ConfigPanel({
  onTriggered,
}: {
  onTriggered: (jobId: string) => void;
}) {
  const qc = useQueryClient();
  const { data: configs = [], isLoading } = useQuery({
    queryKey: ["ml-configs"],
    queryFn: fetchMlConfigs,
  });

  const [filterModel, setFilterModel] = useState("all");
  const [filterDataset, setFilterDataset] = useState("all");
  const [filterRegion, setFilterRegion] = useState("all");
  const [selected, setSelected] = useState<MlConfig | null>(null);
  const [expName, setExpName] = useState("");
  const [error, setError] = useState<string | null>(null);

  const models = [...new Set(configs.map((c) => c.model))].sort();
  const datasets = [...new Set(configs.map((c) => c.dataset))].sort();
  const regions = [...new Set(configs.map((c) => c.region))].sort();

  const filtered = configs.filter(
    (c) =>
      (filterModel === "all" || c.model === filterModel) &&
      (filterDataset === "all" || c.dataset === filterDataset) &&
      (filterRegion === "all" || c.region === filterRegion)
  );

  const trigger = useMutation({
    mutationFn: () => triggerMlRun(selected!.path, expName || undefined),
    onSuccess: (data) => {
      qc.invalidateQueries({ queryKey: ["ml-jobs"] });
      onTriggered(data.job_id);
      setError(null);
    },
    onError: (e: Error) => setError(e.message),
  });

  return (
    <div className="flex flex-col h-full p-4 gap-4">
      <h2 className="text-sm font-semibold text-gray-200 uppercase tracking-wider">
        New Run
      </h2>

      {/* Filters */}
      <div className="grid grid-cols-3 gap-2">
        {[
          { label: "Model", value: filterModel, set: setFilterModel, opts: models },
          { label: "Dataset", value: filterDataset, set: setFilterDataset, opts: datasets },
          { label: "Region", value: filterRegion, set: setFilterRegion, opts: regions },
        ].map(({ label, value, set, opts }) => (
          <div key={label}>
            <label className="block text-xs text-gray-400 mb-1">{label}</label>
            <select
              value={value}
              onChange={(e) => { set(e.target.value); setSelected(null); }}
              className="w-full bg-gray-700 border border-gray-600 rounded px-2 py-1 text-xs text-white"
            >
              <option value="all">All</option>
              {opts.map((o) => (
                <option key={o} value={o}>{o}</option>
              ))}
            </select>
          </div>
        ))}
      </div>

      {/* Config list */}
      <div className="flex-1 overflow-y-auto border border-gray-700 rounded bg-gray-800/50 min-h-0">
        {isLoading ? (
          <p className="text-xs text-gray-400 p-3">Loading configs…</p>
        ) : filtered.length === 0 ? (
          <p className="text-xs text-gray-400 p-3">No configs match filters.</p>
        ) : (
          filtered.map((c) => (
            <button
              key={c.path}
              onClick={() => setSelected(c)}
              className={`w-full text-left px-3 py-2 text-xs border-b border-gray-700 transition-colors ${
                selected?.path === c.path
                  ? "bg-blue-600/30 text-blue-200"
                  : "text-gray-300 hover:bg-gray-700"
              }`}
            >
              <div className="font-medium">{c.model}</div>
              <div className="text-gray-400">
                {c.dataset}
                {c.region !== "CSI" && (
                  <span className="ml-1 text-yellow-400">· {c.region}</span>
                )}
              </div>
            </button>
          ))
        )}
      </div>

      {/* Experiment name */}
      <div>
        <label className="block text-xs text-gray-400 mb-1">
          Experiment name <span className="text-gray-500">(optional)</span>
        </label>
        <input
          value={expName}
          onChange={(e) => setExpName(e.target.value)}
          placeholder="my-nse-run"
          className="w-full bg-gray-700 border border-gray-600 rounded px-2 py-1 text-xs text-white placeholder-gray-500"
        />
      </div>

      {error && (
        <p className="text-xs text-red-400 bg-red-900/20 border border-red-700 rounded px-2 py-1">
          {error}
        </p>
      )}

      {/* Trigger button */}
      <button
        disabled={!selected || trigger.isPending}
        onClick={() => trigger.mutate()}
        className={`w-full py-2 rounded text-sm font-semibold transition-colors ${
          selected && !trigger.isPending
            ? "bg-blue-600 hover:bg-blue-500 text-white"
            : "bg-gray-700 text-gray-500 cursor-not-allowed"
        }`}
      >
        {trigger.isPending ? "Launching…" : selected ? `Run ${selected.model}` : "Select a config"}
      </button>
    </div>
  );
}

// ── Log viewer ─────────────────────────────────────────────────────────────────

function LogViewer({ jobId }: { jobId: string }) {
  const [log, setLog] = useState("");
  const [offset, setOffset] = useState(0);
  const [status, setStatus] = useState<MlJob["status"]>("running");
  const bottomRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    setLog("");
    setOffset(0);
    setStatus("running");
  }, [jobId]);

  useEffect(() => {
    let cancelled = false;

    const poll = async () => {
      try {
        const data = await fetchJobLogs(jobId, offset);
        if (cancelled) return;
        if (data.text) {
          setLog((prev) => prev + data.text);
          setOffset(data.offset);
        }
        setStatus(data.status);
        if (data.status === "running") {
          setTimeout(poll, 1500);
        }
      } catch {
        if (!cancelled) setTimeout(poll, 3000);
      }
    };

    poll();
    return () => { cancelled = true; };
  // Re-start polling when jobId or offset resets
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [jobId]);

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [log]);

  return (
    <div className="flex flex-col h-full">
      <div className="flex items-center px-3 py-1.5 border-b border-gray-700 shrink-0 gap-2">
        <span className="text-xs text-gray-400 font-mono">{jobId}</span>
        <StatusBadge status={status} />
      </div>
      <pre className="flex-1 overflow-y-auto p-3 text-xs font-mono text-gray-300 leading-relaxed whitespace-pre-wrap">
        {log || <span className="text-gray-500">Waiting for output…</span>}
        <div ref={bottomRef} />
      </pre>
    </div>
  );
}

// ── Jobs table ─────────────────────────────────────────────────────────────────

function JobsTable({
  selectedJobId,
  onSelect,
}: {
  selectedJobId: string | null;
  onSelect: (id: string) => void;
}) {
  const { data: jobs = [] } = useQuery({
    queryKey: ["ml-jobs"],
    queryFn: fetchMlJobs,
    refetchInterval: 3000,
  });

  const fmt = (iso: string | null) => {
    if (!iso) return "—";
    const d = new Date(iso + "Z");
    return d.toLocaleTimeString();
  };

  const elapsed = (job: MlJob) => {
    const start = new Date(job.started_at + "Z").getTime();
    const end = job.finished_at
      ? new Date(job.finished_at + "Z").getTime()
      : Date.now();
    const s = Math.round((end - start) / 1000);
    if (s < 60) return `${s}s`;
    const m = Math.floor(s / 60);
    return `${m}m ${s % 60}s`;
  };

  if (jobs.length === 0) {
    return (
      <div className="flex items-center justify-center h-full text-xs text-gray-500">
        No runs yet. Trigger a run from the left panel.
      </div>
    );
  }

  return (
    <div className="overflow-y-auto h-full">
      <table className="w-full text-xs">
        <thead>
          <tr className="text-gray-400 border-b border-gray-700 sticky top-0 bg-gray-900">
            <th className="text-left px-3 py-2">Model</th>
            <th className="text-left px-3 py-2">Config</th>
            <th className="text-left px-3 py-2">Status</th>
            <th className="text-left px-3 py-2">Started</th>
            <th className="text-left px-3 py-2">Elapsed</th>
            <th className="text-left px-3 py-2">Logs</th>
          </tr>
        </thead>
        <tbody>
          {jobs.map((job) => (
            <tr
              key={job.job_id}
              onClick={() => onSelect(job.job_id)}
              className={`border-b border-gray-800 cursor-pointer transition-colors ${
                selectedJobId === job.job_id
                  ? "bg-blue-900/20"
                  : "hover:bg-gray-800"
              }`}
            >
              <td className="px-3 py-2 font-medium text-gray-200">{job.model}</td>
              <td className="px-3 py-2 text-gray-400 max-w-[160px] truncate" title={job.label}>
                {job.label}
              </td>
              <td className="px-3 py-2">
                <StatusBadge status={job.status} />
              </td>
              <td className="px-3 py-2 text-gray-400 font-mono">{fmt(job.started_at)}</td>
              <td className="px-3 py-2 text-gray-400 font-mono">{elapsed(job)}</td>
              <td className="px-3 py-2">
                <button
                  onClick={(e) => { e.stopPropagation(); onSelect(job.job_id); }}
                  className="text-blue-400 hover:text-blue-300 underline"
                >
                  view
                </button>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

// ── Page root ──────────────────────────────────────────────────────────────────

export default function MLRunsPage() {
  const [selectedJobId, setSelectedJobId] = useState<string | null>(null);

  return (
    <div className="flex flex-1 overflow-hidden">
      {/* Left: config selector */}
      <aside className="w-64 shrink-0 border-r border-gray-700 overflow-hidden flex flex-col">
        <ConfigPanel onTriggered={(id) => setSelectedJobId(id)} />
      </aside>

      {/* Right: jobs + log */}
      <div className="flex flex-1 flex-col overflow-hidden">
        {/* Jobs table (top half) */}
        <div className="flex-1 overflow-hidden border-b border-gray-700">
          <div className="px-3 py-2 border-b border-gray-700 shrink-0">
            <h2 className="text-xs font-semibold text-gray-400 uppercase tracking-wider">
              Runs
            </h2>
          </div>
          <div className="h-full overflow-hidden" style={{ height: "calc(100% - 33px)" }}>
            <JobsTable selectedJobId={selectedJobId} onSelect={setSelectedJobId} />
          </div>
        </div>

        {/* Log viewer (bottom half) */}
        <div className="h-1/2 shrink-0 flex flex-col overflow-hidden bg-gray-950">
          {selectedJobId ? (
            <LogViewer jobId={selectedJobId} />
          ) : (
            <div className="flex items-center justify-center h-full text-xs text-gray-500">
              Select a run to view logs
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
