import { useQuery } from "@tanstack/react-query";
import { fetchInstruments } from "../api/client";

interface Props {
  market: string;
  onMarketChange: (m: string) => void;
  selected: string;
  onSelect: (symbol: string) => void;
  compareSymbols: string[];
  onToggleCompare: (symbol: string) => void;
}

const MARKETS = ["nifty50", "nifty500", "all"];

export function StockSelector({
  market,
  onMarketChange,
  selected,
  onSelect,
  compareSymbols,
  onToggleCompare,
}: Props) {
  const { data: instruments = [], isLoading } = useQuery({
    queryKey: ["instruments", market],
    queryFn: () => fetchInstruments(market),
  });

  return (
    <div className="flex flex-col gap-2 h-full">
      <div className="flex gap-2">
        {MARKETS.map((m) => (
          <button
            key={m}
            onClick={() => onMarketChange(m)}
            className={`px-3 py-1 rounded text-sm font-medium transition-colors ${
              market === m
                ? "bg-blue-600 text-white"
                : "bg-gray-700 text-gray-300 hover:bg-gray-600"
            }`}
          >
            {m.toUpperCase()}
          </button>
        ))}
      </div>

      {isLoading ? (
        <p className="text-gray-400 text-sm">Loading…</p>
      ) : (
        <ul className="flex-1 overflow-y-auto rounded border border-gray-700 bg-gray-800">
          {instruments.map((sym) => (
            <li
              key={sym}
              className={`flex items-center justify-between px-3 py-1.5 cursor-pointer text-sm border-b border-gray-700 last:border-0 hover:bg-gray-700 transition-colors ${
                selected === sym ? "bg-blue-900 text-white" : "text-gray-200"
              }`}
              onClick={() => onSelect(sym)}
            >
              <span>{sym}</span>
              <button
                onClick={(e) => {
                  e.stopPropagation();
                  onToggleCompare(sym);
                }}
                title={
                  compareSymbols.includes(sym)
                    ? "Remove from comparison"
                    : "Add to comparison"
                }
                className={`text-xs px-1.5 py-0.5 rounded ${
                  compareSymbols.includes(sym)
                    ? "bg-green-600 text-white"
                    : "bg-gray-600 text-gray-300"
                }`}
              >
                {compareSymbols.includes(sym) ? "✓" : "+"}
              </button>
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}
