import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { fetchSignals, SignalStats } from "../api/client";
import { InfoTooltip } from "./InfoTooltip";

interface Props {
  onSelect: (symbol: string) => void;
}

function zScore(value: number, stats: SignalStats): number {
  if (stats.std === 0) return 0;
  return (value - stats.mean) / stats.std;
}

function percentileRank(value: number, stats: SignalStats): number {
  // Approximate percentile using normal CDF approximation from z-score
  const z = zScore(value, stats);
  const t = 1 / (1 + 0.2316419 * Math.abs(z));
  const d = 0.3989423 * Math.exp((-z * z) / 2);
  const p =
    d *
    t *
    (0.3193815 +
      t * (-0.3565638 + t * (1.7814779 + t * (-1.8212559 + t * 1.3302744))));
  return z >= 0 ? Math.round((1 - p) * 100) : Math.round(p * 100);
}

function zColor(z: number, tab: "top" | "bottom"): string {
  if (tab === "top") {
    if (z >= 2) return "#10b981";
    if (z >= 1) return "#34d399";
    return "#6ee7b7";
  } else {
    if (z <= -2) return "#f87171";
    if (z <= -1) return "#ef5350";
    return "#fca5a5";
  }
}

export function SignalRankings({ onSelect }: Props) {
  const [date, setDate] = useState<string | undefined>();
  const [topn, setTopn] = useState(10);
  const [tab, setTab] = useState<"top" | "bottom">("top");

  const { data, isLoading, error } = useQuery({
    queryKey: ["signals", date, topn],
    queryFn: () => fetchSignals(date, topn),
  });

  const rows = tab === "top" ? data?.top ?? [] : data?.bottom ?? [];
  const stats = data?.stats;
  const maxAbs = Math.max(...rows.map((r) => Math.abs(r.score)), 0.001);

  return (
    <div className="h-full flex flex-col">
      <div className="flex items-center gap-2 px-2 py-1 shrink-0">
        <h2 className="text-sm font-semibold text-gray-300 flex items-center">
          Alpha Signals
          <InfoTooltip
            content="Alpha signals are model-predicted scores for each stock. A higher score means the model expects stronger relative outperformance. Scores are derived from 158 price/volume factors (Alpha158) via a LightGBM model. Percentile shown per stock is relative to the full universe on that day."
            width="w-80"
          />
        </h2>
        <span className="text-xs text-gray-500">{data?.date ?? "…"}</span>
        <div className="ml-auto flex items-center gap-2">
          <select
            className="text-xs bg-gray-700 text-gray-200 rounded px-1 py-0.5"
            value={date ?? ""}
            onChange={(e) => setDate(e.target.value || undefined)}
          >
            <option value="">Latest</option>
            {data?.available_dates.slice().reverse().map((d) => (
              <option key={d} value={d}>{d}</option>
            ))}
          </select>
          <select
            className="text-xs bg-gray-700 text-gray-200 rounded px-1 py-0.5"
            value={topn}
            onChange={(e) => setTopn(Number(e.target.value))}
          >
            {[5, 10, 20, 50].map((n) => (
              <option key={n} value={n}>Top {n}</option>
            ))}
          </select>
        </div>
      </div>

      {/* Distribution stats bar */}
      {stats && (
        <div className="px-2 py-1 shrink-0 bg-gray-800/60 border-b border-gray-700 text-xs text-gray-400 flex items-center gap-3 flex-wrap">
          <span>
            <span className="text-gray-500">Universe</span>{" "}
            <span className="text-gray-200 font-medium">{stats.count.toLocaleString()}</span> stocks
          </span>
          <span>
            <span className="text-gray-500">μ</span>{" "}
            <span className={stats.mean >= 0 ? "text-emerald-400" : "text-red-400"}>
              {stats.mean >= 0 ? "+" : ""}{stats.mean.toFixed(4)}
            </span>
          </span>
          <span>
            <span className="text-gray-500">σ</span>{" "}
            <span className="text-gray-300">{stats.std.toFixed(4)}</span>
          </span>
          <span>
            <span className="text-gray-500">range</span>{" "}
            <span className="text-red-400">{stats.min.toFixed(3)}</span>
            <span className="text-gray-600"> → </span>
            <span className="text-emerald-400">{stats.max.toFixed(3)}</span>
          </span>
        </div>
      )}

      <div className="flex gap-0 shrink-0 border-b border-gray-700">
        {(["top", "bottom"] as const).map((t) => (
          <button
            key={t}
            onClick={() => setTab(t)}
            className={`flex-1 py-1 text-xs font-medium transition-colors ${
              tab === t
                ? "bg-gray-700 text-white"
                : "text-gray-400 hover:text-gray-200"
            }`}
          >
            {t === "top" ? `▲ Long (top ${topn})` : `▼ Short (bottom ${topn})`}
          </button>
        ))}
      </div>

      {isLoading && <Placeholder text="Loading…" />}
      {error && <Placeholder text={`Error: ${(error as Error).message}`} />}

      {!isLoading && !error && (
        <ul className="flex-1 overflow-y-auto">
          {rows.map((item, i) => {
            const pct = item.score / maxAbs;
            const z = stats ? zScore(item.score, stats) : 0;
            const pctile = stats ? percentileRank(item.score, stats) : null;
            const barColor = stats ? zColor(z, tab) : (tab === "top" ? "#26a69a" : "#ef5350");
            return (
              <li
                key={item.symbol}
                className="flex items-center gap-2 px-2 py-1 border-b border-gray-800 cursor-pointer hover:bg-gray-800 transition-colors"
                onClick={() => onSelect(item.symbol)}
                title={`z-score: ${z.toFixed(2)}${pctile !== null ? ` · ${pctile}th percentile` : ""}`}
              >
                <span className="text-xs text-gray-500 w-5 shrink-0">{i + 1}</span>
                <span className="text-xs text-gray-200 w-24 shrink-0 truncate">{item.symbol}</span>
                <div className="flex-1 h-3 bg-gray-800 rounded-sm overflow-hidden">
                  <div
                    className="h-full rounded-sm transition-all"
                    style={{
                      width: `${Math.abs(pct) * 100}%`,
                      backgroundColor: barColor,
                      opacity: 0.85,
                    }}
                  />
                </div>
                <span className="text-xs w-16 text-right shrink-0" style={{ color: barColor }}>
                  {item.score.toFixed(4)}
                </span>
                {pctile !== null && (
                  <span className="text-xs text-gray-500 w-9 text-right shrink-0 font-mono">
                    p{pctile}
                  </span>
                )}
              </li>
            );
          })}
        </ul>
      )}
    </div>
  );
}

function Placeholder({ text }: { text: string }) {
  return (
    <div className="flex-1 flex items-center justify-center text-gray-500 text-sm">
      {text}
    </div>
  );
}
