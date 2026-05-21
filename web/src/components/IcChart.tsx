import ReactECharts from "echarts-for-react";
import { useQuery } from "@tanstack/react-query";
import { fetchIc } from "../api/client";
import { InfoTooltip } from "./InfoTooltip";

function rollingMean(arr: number[], window: number): (number | null)[] {
  return arr.map((_, i) => {
    if (i < window - 1) return null;
    const slice = arr.slice(i - window + 1, i + 1);
    return slice.reduce((a, b) => a + b, 0) / slice.length;
  });
}

function icColor(ic: number): string {
  if (ic >= 0.05) return "text-emerald-400";
  if (ic >= 0.03) return "text-amber-400";
  return "text-red-400";
}

function icirColor(icir: number): string {
  if (icir >= 1.0) return "text-emerald-400";
  if (icir >= 0.5) return "text-amber-400";
  return "text-red-400";
}

function icLabel(ic: number): string {
  if (ic >= 0.05) return "strong";
  if (ic >= 0.03) return "moderate";
  return "weak";
}

function icirLabel(icir: number): string {
  if (icir >= 1.0) return "high";
  if (icir >= 0.5) return "fair";
  return "low";
}

export function IcChart() {
  const { data, isLoading, error } = useQuery({
    queryKey: ["ic"],
    queryFn: () => fetchIc(),
  });

  if (isLoading) return <Placeholder text="Loading IC data…" />;
  if (error) return <Placeholder text={`Error: ${(error as Error).message}`} />;
  if (!data) return <Placeholder text="No experiment data" />;

  const rolling = rollingMean(data.ic, 21);

  const option = {
    backgroundColor: "transparent",
    animation: false,
    tooltip: {
      trigger: "axis",
      formatter: (params: any[]) =>
        params
          .filter((p) => p.data[1] !== null && p.data[1] !== undefined)
          .map((p) => `${p.marker}${p.seriesName}: ${(p.data[1] as number).toFixed(4)}`)
          .join("<br/>"),
    },
    legend: { top: 4, textStyle: { color: "#ccc" } },
    grid: { left: 50, right: 80, top: 40, bottom: 60 },
    xAxis: {
      type: "time",
      axisLabel: { color: "#aaa" },
      axisLine: { lineStyle: { color: "#555" } },
    },
    yAxis: {
      scale: true,
      axisLabel: { color: "#aaa", formatter: (v: number) => v.toFixed(2) },
      splitLine: { lineStyle: { color: "#333" } },
    },
    dataZoom: [
      { type: "inside" },
      { type: "slider", bottom: 10, height: 20, textStyle: { color: "#aaa" } },
    ],
    series: [
      {
        name: "IC",
        type: "bar",
        data: data.dates.map((d, i) => [d, data.ic[i]]),
        itemStyle: {
          color: (p: any) => (p.data[1] >= 0 ? "#26a69a" : "#ef5350"),
        },
        markLine: {
          silent: true,
          symbol: ["none", "none"],
          data: [
            {
              name: "IC=0.05",
              yAxis: 0.05,
              lineStyle: { color: "#26a69a", type: "dashed", opacity: 0.7 },
              label: { show: true, formatter: "0.05 (strong)", color: "#26a69a", fontSize: 9, position: "end" },
            },
            {
              name: "IC=0.03",
              yAxis: 0.03,
              lineStyle: { color: "#fac858", type: "dashed", opacity: 0.7 },
              label: { show: true, formatter: "0.03 (moderate)", color: "#fac858", fontSize: 9, position: "end" },
            },
            {
              name: "IC=-0.05",
              yAxis: -0.05,
              lineStyle: { color: "#ef5350", type: "dashed", opacity: 0.5 },
              label: { show: false },
            },
          ],
        },
      },
      ...(data.ric.length
        ? [
            {
              name: "Rank IC",
              type: "line",
              smooth: true,
              symbol: "none",
              lineStyle: { color: "#fac858", width: 1.5 },
              data: data.dates.map((d, i) => [d, data.ric[i]]),
            },
          ]
        : []),
      {
        name: "21d Avg IC",
        type: "line",
        smooth: false,
        symbol: "none",
        lineStyle: { color: "#60a5fa", width: 1.5, type: "dashed" },
        data: data.dates
          .map((d, i) => [d, rolling[i]])
          .filter((p) => p[1] !== null),
      },
    ],
  };

  return (
    <div className="h-full flex flex-col">
      <div className="flex items-center gap-4 px-2 py-1 flex-wrap">
        <h2 className="text-sm font-semibold text-gray-300 flex items-center">
          IC over Time
          <InfoTooltip
            content="Information Coefficient (IC) measures the daily rank correlation between predicted scores and actual next-day returns. IC > 0.05 is considered strong predictive power. ICIR (IC / std) measures signal consistency."
            width="w-72"
          />
        </h2>
        <span className="text-xs text-gray-400">
          Mean IC:{" "}
          <b className={icColor(data.mean_ic)}>
            {data.mean_ic.toFixed(4)}
          </b>
          <span className={`ml-1 text-xs ${icColor(data.mean_ic)}`}>
            ({icLabel(data.mean_ic)})
          </span>
          <InfoTooltip
            content={`Mean IC = ${data.mean_ic.toFixed(4)}. Threshold: < 0.03 = weak, 0.03–0.05 = moderate, > 0.05 = strong.`}
          />
          &nbsp;·&nbsp; ICIR:{" "}
          <b className={icirColor(data.icir)}>{data.icir.toFixed(3)}</b>
          <span className={`ml-1 text-xs ${icirColor(data.icir)}`}>
            ({icirLabel(data.icir)})
          </span>
          <InfoTooltip
            content={`ICIR = Mean IC / Std IC = ${data.icir.toFixed(3)}. Measures signal stability. ICIR > 1.0 is considered high. Dashed reference lines at IC = 0.03 and 0.05 mark quality thresholds.`}
          />
        </span>
      </div>
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
