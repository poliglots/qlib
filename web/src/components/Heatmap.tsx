import ReactECharts from "echarts-for-react";
import { useQuery } from "@tanstack/react-query";
import { fetchHeatmap } from "../api/client";

interface Props {
  market: string;
  onSelect: (symbol: string) => void;
}

const PERIOD_OPTIONS = [
  { label: "1W", days: 5 },
  { label: "1M", days: 21 },
  { label: "3M", days: 63 },
];

export function Heatmap({ market, onSelect }: Props) {
  const [days, setDays] = React.useState(5);

  const { data, isLoading, error } = useQuery({
    queryKey: ["heatmap", market, days],
    queryFn: () => fetchHeatmap(market, days),
  });

  const option = React.useMemo(() => {
    if (!data?.length) return {};

    // Build a square-ish grid
    const n = data.length;
    const cols = Math.ceil(Math.sqrt(n * 1.6));
    const rows = Math.ceil(n / cols);

    const xLabels = Array.from({ length: cols }, (_, i) => String(i));
    const yLabels = Array.from({ length: rows }, (_, i) => String(i));

    const maxAbs = Math.max(...data.map((d) => Math.abs(d.change_pct)), 0.01);

    const heatData = data.map((item, idx) => {
      const col = idx % cols;
      const row = Math.floor(idx / cols);
      return [col, row, item.change_pct, item.symbol];
    });

    return {
      backgroundColor: "transparent",
      animation: false,
      tooltip: {
        formatter: (p: any) => {
          const [, , pct, sym] = p.data as [number, number, number, string];
          return `<b>${sym}</b><br/>${pct >= 0 ? "+" : ""}${pct.toFixed(2)}%`;
        },
      },
      grid: { left: 10, right: 10, top: 10, bottom: 10 },
      xAxis: {
        type: "category",
        data: xLabels,
        axisLabel: { show: false },
        axisTick: { show: false },
        axisLine: { show: false },
        splitLine: { show: false },
      },
      yAxis: {
        type: "category",
        data: yLabels,
        axisLabel: { show: false },
        axisTick: { show: false },
        axisLine: { show: false },
        splitLine: { show: false },
      },
      visualMap: {
        min: -maxAbs,
        max: maxAbs,
        calculable: false,
        show: false,
        inRange: { color: ["#c62828", "#424242", "#2e7d32"] },
      },
      series: [
        {
          type: "heatmap",
          data: heatData,
          label: {
            show: true,
            formatter: (p: any) => {
              const [, , pct, sym] = p.data as [number, number, number, string];
              const ticker = sym.replace(".NS", "");
              return `{name|${ticker}}\n{pct|${pct >= 0 ? "+" : ""}${pct.toFixed(1)}%}`;
            },
            rich: {
              name: { fontSize: 9, color: "#fff", fontWeight: "bold" },
              pct: { fontSize: 8, color: "#ddd" },
            },
          },
          itemStyle: { borderWidth: 1, borderColor: "#111" },
          emphasis: { itemStyle: { borderColor: "#fff", borderWidth: 2 } },
        },
      ],
    };
  }, [data]);

  const handleClick = (params: any) => {
    if (params?.data?.[3]) onSelect(params.data[3] as string);
  };

  return (
    <div className="h-full flex flex-col">
      <div className="flex items-center gap-2 px-2 py-1">
        <h2 className="text-sm font-semibold text-gray-300">
          Performance Heatmap
        </h2>
        <div className="flex gap-1 ml-auto">
          {PERIOD_OPTIONS.map((opt) => (
            <button
              key={opt.days}
              onClick={() => setDays(opt.days)}
              className={`px-2 py-0.5 text-xs rounded ${
                days === opt.days
                  ? "bg-blue-600 text-white"
                  : "bg-gray-700 text-gray-300 hover:bg-gray-600"
              }`}
            >
              {opt.label}
            </button>
          ))}
        </div>
      </div>

      {isLoading && <Placeholder text="Loading…" />}
      {error && <Placeholder text={`Error: ${(error as Error).message}`} />}
      {!isLoading && !error && !data?.length && <Placeholder text="No data" />}
      {!isLoading && !error && !!data?.length && (
        <div className="flex-1">
          <ReactECharts
            option={option}
            style={{ height: "100%", width: "100%" }}
            theme="dark"
            onEvents={{ click: handleClick }}
          />
        </div>
      )}
    </div>
  );
}

import React from "react";

function Placeholder({ text }: { text: string }) {
  return (
    <div className="flex-1 flex items-center justify-center text-gray-500 text-sm">
      {text}
    </div>
  );
}
