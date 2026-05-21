import ReactECharts from "echarts-for-react";
import { useQuery } from "@tanstack/react-query";
import { fetchOhlcv } from "../api/client";

interface Props {
  symbol: string;
  start: string;
  end: string;
}

export function CandlestickChart({ symbol, start, end }: Props) {
  const { data, isLoading, error } = useQuery({
    queryKey: ["ohlcv", symbol, start, end],
    queryFn: () => fetchOhlcv(symbol, start, end),
    enabled: !!symbol,
  });

  if (!symbol) return <Placeholder text="Select a stock" />;
  if (isLoading) return <Placeholder text="Loading…" />;
  if (error) return <Placeholder text={`Error: ${(error as Error).message}`} />;
  if (!data?.length) return <Placeholder text="No data" />;

  const dates = data.map((r) => r.date);
  const ohlc = data.map((r) => [r.open, r.close, r.low, r.high]);
  const volumes = data.map((r) => r.volume);

  const option = {
    backgroundColor: "transparent",
    animation: false,
    tooltip: {
      trigger: "axis",
      axisPointer: { type: "cross" },
      formatter: (params: any[]) => {
        const c = params.find((p) => p.seriesName === "Price");
        const v = params.find((p) => p.seriesName === "Volume");
        if (!c) return "";
        const [o, cl, low, high] = c.data as number[];
        return `
          <b>${c.name}</b><br/>
          Open: ${o.toFixed(2)}<br/>
          Close: ${cl.toFixed(2)}<br/>
          High: ${high.toFixed(2)}<br/>
          Low: ${low.toFixed(2)}<br/>
          ${v ? `Volume: ${v.data.toLocaleString()}` : ""}
        `;
      },
    },
    legend: { top: 4, textStyle: { color: "#ccc" } },
    grid: [
      { left: 60, right: 20, top: 40, bottom: 120 },
      { left: 60, right: 20, top: "75%", bottom: 60 },
    ],
    xAxis: [
      {
        type: "category",
        data: dates,
        gridIndex: 0,
        axisLabel: { show: false },
        axisLine: { lineStyle: { color: "#555" } },
      },
      {
        type: "category",
        data: dates,
        gridIndex: 1,
        axisLabel: { color: "#aaa", fontSize: 10 },
        axisLine: { lineStyle: { color: "#555" } },
      },
    ],
    yAxis: [
      {
        scale: true,
        gridIndex: 0,
        splitLine: { lineStyle: { color: "#333" } },
        axisLabel: { color: "#aaa" },
      },
      {
        gridIndex: 1,
        splitLine: { show: false },
        axisLabel: { color: "#aaa", fontSize: 10 },
      },
    ],
    dataZoom: [
      { type: "inside", xAxisIndex: [0, 1], start: 60, end: 100 },
      {
        type: "slider",
        xAxisIndex: [0, 1],
        bottom: 10,
        height: 20,
        textStyle: { color: "#aaa" },
      },
    ],
    series: [
      {
        name: "Price",
        type: "candlestick",
        xAxisIndex: 0,
        yAxisIndex: 0,
        data: ohlc,
        itemStyle: {
          color: "#26a69a",
          color0: "#ef5350",
          borderColor: "#26a69a",
          borderColor0: "#ef5350",
        },
      },
      {
        name: "Volume",
        type: "bar",
        xAxisIndex: 1,
        yAxisIndex: 1,
        data: volumes,
        itemStyle: { color: "#5c6bc0" },
      },
    ],
  };

  return (
    <div className="h-full flex flex-col">
      <h2 className="text-sm font-semibold text-gray-300 px-2 py-1">
        {symbol} — OHLCV
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
    <div className="h-full flex items-center justify-center text-gray-500">
      {text}
    </div>
  );
}
