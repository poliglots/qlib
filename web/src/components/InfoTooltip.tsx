import { useState } from "react";

interface Props {
  content: string;
  width?: string;
}

export function InfoTooltip({ content, width = "w-64" }: Props) {
  const [visible, setVisible] = useState(false);

  return (
    <span
      className="relative inline-block"
      onMouseEnter={() => setVisible(true)}
      onMouseLeave={() => setVisible(false)}
    >
      <span className="text-gray-500 hover:text-gray-300 cursor-help text-xs font-normal select-none ml-1">
        ⓘ
      </span>
      {visible && (
        <div
          className={`absolute z-50 left-5 top-0 ${width} bg-gray-700 border border-gray-600 rounded p-2.5 text-xs text-gray-200 shadow-xl leading-relaxed pointer-events-none`}
        >
          {content}
        </div>
      )}
    </span>
  );
}
