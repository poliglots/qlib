import { useState } from "react";
import { StockSelector } from "./components/StockSelector";
import { CandlestickChart } from "./components/CandlestickChart";
import { ComparisonChart } from "./components/ComparisonChart";
import { Heatmap } from "./components/Heatmap";
import { IcChart } from "./components/IcChart";
import { SignalRankings } from "./components/SignalRankings";
import MLRunsPage from "./components/MLRunsPage";

const DEFAULT_START = "2022-01-01";
const DEFAULT_END = new Date().toISOString().slice(0, 10);

type RightPanel = "comparison" | "signals";
type Page = "dashboard" | "ml-runs";

export default function App() {
  const [page, setPage] = useState<Page>("dashboard");
  const [market, setMarket] = useState("nifty50");
  const [selected, setSelected] = useState("");
  const [compareSymbols, setCompareSymbols] = useState<string[]>([]);
  const [start] = useState(DEFAULT_START);
  const [end] = useState(DEFAULT_END);
  const [rightPanel, setRightPanel] = useState<RightPanel>("signals");

  const toggleCompare = (sym: string) => {
    setCompareSymbols((prev) =>
      prev.includes(sym) ? prev.filter((s) => s !== sym) : [...prev, sym]
    );
  };

  return (
    <div className="flex flex-col h-screen bg-gray-900 text-white">
      {/* Header */}
      <header className="flex items-center px-4 py-2 bg-gray-800 border-b border-gray-700 shrink-0 gap-4">
        <h1 className="text-lg font-bold tracking-wide shrink-0">NSE</h1>
        {/* Nav tabs */}
        <nav className="flex gap-1">
          {(["dashboard", "ml-runs"] as Page[]).map((p) => (
            <button
              key={p}
              onClick={() => setPage(p)}
              className={`px-3 py-1 rounded text-xs font-medium transition-colors ${
                page === p
                  ? "bg-blue-600 text-white"
                  : "text-gray-400 hover:text-gray-200 hover:bg-gray-700"
              }`}
            >
              {p === "dashboard" ? "Dashboard" : "ML Runs"}
            </button>
          ))}
        </nav>
        {page === "dashboard" && (
          <span className="ml-auto text-xs text-gray-400">
            {start} → {end}
          </span>
        )}
      </header>

      {/* ML Runs page */}
      {page === "ml-runs" && <MLRunsPage />}

      {/* Dashboard */}
      {page === "dashboard" && <div className="flex flex-1 overflow-hidden">
        {/* Left: stock selector */}
        <aside className="w-52 shrink-0 border-r border-gray-700 p-2 flex flex-col overflow-hidden">
          <StockSelector
            market={market}
            onMarketChange={setMarket}
            selected={selected}
            onSelect={setSelected}
            compareSymbols={compareSymbols}
            onToggleCompare={toggleCompare}
          />
        </aside>

        {/* Centre + right column */}
        <div className="flex flex-1 flex-col overflow-hidden">
          {/* Top row: candlestick + right panel */}
          <div className="flex flex-1 overflow-hidden border-b border-gray-700">
            {/* Candlestick */}
            <div className="flex-1 border-r border-gray-700 overflow-hidden">
              <CandlestickChart symbol={selected} start={start} end={end} />
            </div>

            {/* Right panel: comparison or signal rankings */}
            <div className="w-96 shrink-0 flex flex-col overflow-hidden">
              {/* Tab switcher */}
              <div className="flex border-b border-gray-700 shrink-0">
                {(["signals", "comparison"] as RightPanel[]).map((p) => (
                  <button
                    key={p}
                    onClick={() => setRightPanel(p)}
                    className={`flex-1 py-1.5 text-xs font-medium transition-colors ${
                      rightPanel === p
                        ? "bg-gray-700 text-white border-b-2 border-blue-500"
                        : "text-gray-400 hover:text-gray-200"
                    }`}
                  >
                    {p === "signals" ? "Alpha Signals" : "Comparison"}
                  </button>
                ))}
              </div>
              <div className="flex-1 overflow-hidden">
                {rightPanel === "comparison" ? (
                  <ComparisonChart
                    symbols={compareSymbols}
                    start={start}
                    end={end}
                  />
                ) : (
                  <SignalRankings onSelect={setSelected} />
                )}
              </div>
            </div>
          </div>

          {/* Bottom row: IC chart + heatmap */}
          <div className="flex h-64 shrink-0 overflow-hidden">
            <div className="flex-1 border-r border-gray-700 overflow-hidden">
              <IcChart />
            </div>
            <div className="w-96 shrink-0 overflow-hidden">
              <Heatmap
                market={market}
                onSelect={(sym) => setSelected(sym)}
              />
            </div>
          </div>
        </div>
      </div>}
    </div>
  );
}
