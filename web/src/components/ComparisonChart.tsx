import ReactECharts from "echarts-for-react";
import { useQuery } from "@tanstack/react-query";
import { fetchCompare } from "../api/client";

interface Props {
  symbols: string[];
  start: string;
  end: string;
}

const COLORS = [
  "#5470c6", "#91cc75", "#fac858", "#ee6666",
  "#73c0de", "#3ba272", "#fc8452", "#9a60b4",
];

export function ComparisonChart({ symbols, start, end }: Props) {
  const { data, isLoading, error } = useQuery({
    queryKey: ["compare", symbols.join(","), start, end],
    queryFn: () => fetchCompare(symbols, start, end),
    enabled: symbols.length > 0,
  });

  if (!symbols.length)
    return <Placeholder text="Add stocks to comparison with +" />;
  if (isLoading) return <Placeholder text="Loading…" />;
  if (error) return <Placeholder text={`Error: ${(error as Error).message}`} />;

  const series = Object.entries(data ?? {}).map(([sym, pts], i) => ({
    name: sym,
    type: "line",
    smooth: false,
    symbol: "none",
    color: COLORS[i % COLORS.length],
    data: pts.map((p) => [p.date, p.value]),
  }));

  const option = {
    backgroundColor: "transparent",
    animation: false,
    tooltip: {
      trigger: "axis",
      formatter: (params: any[]) =>
        params
          .map((p) => `${p.marker}${p.seriesName}: ${(p.data[1] as number).toFixed(2)}`)
          .join("<br/>"),
    },
    legend: { top: 4, textStyle: { color: "#ccc" } },
    grid: { left: 50, right: 20, top: 40, bottom: 60 },
    xAxis: {
      type: "time",
      axisLabel: { color: "#aaa" },
      axisLine: { lineStyle: { color: "#555" } },
    },
    yAxis: {
      scale: true,
      axisLabel: {
        color: "#aaa",
        formatter: (v: number) => v.toFixed(0),
      },
      splitLine: { lineStyle: { color: "#333" } },
      name: "Normalised (base=100)",
      nameTextStyle: { color: "#777", fontSize: 10 },
    },
    dataZoom: [
      { type: "inside" },
      { type: "slider", bottom: 10, height: 20, textStyle: { color: "#aaa" } },
    ],
    series,
  };

  return (
    <div className="h-full flex flex-col">
      <h2 className="text-sm font-semibold text-gray-300 px-2 py-1">
        Comparison (normalised to 100)
      </h2>
      <div className="flex-1">
        <ReactECharts
          option={option}
          style={{ height: "100%", width: "100%" }}
          theme="dark"
        />
      </div>
    </div>
  );
}

function Placeholder({ text }: { text: string }) {
  return (
    <div className="h-full flex items-center justify-center text-gray-500 text-sm">
      {text}
    </div>
  );
}
