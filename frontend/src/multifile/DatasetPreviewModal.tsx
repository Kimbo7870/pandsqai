import { useEffect, useMemo, useRef, useState } from "react";
import { getMultiChunk } from "../lib/multifileApi";
import type { CellLike, MultiChunkResponse } from "../lib/multifileTypes";

export type PreviewDataset = {
  dataset_id: string;
  display_name: string;
};

type Props = {
  open: boolean;
  onClose: () => void;
  dataset: PreviewDataset | null;
  previewRows: number;
  previewCols: number;
};

function cellToText(v: CellLike): string {
  if (v === null) return "";
  return String(v);
}

export default function DatasetPreviewModal({
  open,
  onClose,
  dataset,
  previewRows,
  previewCols,
}: Props) {
  const [loading, setLoading] = useState<boolean>(false);
  const [err, setErr] = useState<string>("");
  const [chunk, setChunk] = useState<MultiChunkResponse | null>(null);

  const abortRef = useRef<AbortController | null>(null);

  const title = useMemo(() => {
    if (!dataset) return "Preview";
    return `Preview — ${dataset.display_name}`;
  }, [dataset]);

  useEffect(() => {
    if (!open) return;

    function onKeyDown(e: KeyboardEvent) {
      if (e.key === "Escape") onClose();
    }

    window.addEventListener("keydown", onKeyDown);
    return () => window.removeEventListener("keydown", onKeyDown);
  }, [open, onClose]);

  useEffect(() => {
    if (!open || !dataset) {
      setChunk(null);
      setErr("");
      setLoading(false);
      abortRef.current?.abort();
      abortRef.current = null;
      return;
    }

    const ac = new AbortController();
    abortRef.current?.abort();
    abortRef.current = ac;

    setLoading(true);
    setErr("");
    setChunk(null);

    (async () => {
      try {
        const j = await getMultiChunk(
          dataset.dataset_id,
          0,
          0,
          previewRows,
          previewCols,
          ac.signal
        );
        setChunk(j);
      } catch (e) {
        if (e instanceof DOMException && e.name === "AbortError") return;
        setErr(e instanceof Error ? e.message : "Failed to load preview");
      } finally {
        setLoading(false);
      }
    })();

    return () => ac.abort();
  }, [open, dataset, previewRows, previewCols]);

  if (!open) return null;

  return (
    <div
      className="fixed inset-0 z-50 flex items-center justify-center bg-black/40"
      onMouseDown={onClose}
      role="dialog"
      aria-modal="true"
    >
      <div
        className="w-[92vw] max-w-5xl bg-white rounded-lg shadow-xl border border-gray-200"
        onMouseDown={(e) => e.stopPropagation()}
      >
        <div className="flex items-center justify-between px-4 py-3 border-b border-gray-200">
          <div className="min-w-0">
            <h2 className="text-lg font-semibold text-gray-900 truncate">{title}</h2>
            <p className="text-xs text-gray-600">
              Showing up to {previewRows}×{previewCols}
            </p>
          </div>
          <button
            className="px-3 py-1 rounded border border-gray-300 text-gray-800 hover:bg-gray-100"
            onClick={onClose}
            type="button"
          >
            Close
          </button>
        </div>

        <div className="p-4">
          {loading ? (
            <div className="text-sm text-gray-600">Loading…</div>
          ) : err ? (
            <div className="text-sm text-gray-700 whitespace-pre-wrap">{err}</div>
          ) : !chunk ? (
            <div className="text-sm text-gray-600">No data.</div>
          ) : (
            <div className="border border-gray-200 rounded overflow-auto max-h-[70vh]">
              <table className="min-w-full text-sm">
                <thead className="sticky top-0 bg-gray-50 border-b border-gray-200">
                  <tr>
                    {chunk.columns.map((c) => (
                      <th
                        key={c}
                        className="text-left font-medium text-gray-700 px-3 py-2 whitespace-nowrap"
                      >
                        {c}
                      </th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {chunk.rows.map((row, i) => (
                    <tr key={i} className="odd:bg-white even:bg-gray-50">
                      {row.map((cell, j) => (
                        <td
                          key={j}
                          className="px-3 py-2 border-t border-gray-100 whitespace-nowrap max-w-[260px] overflow-hidden text-ellipsis"
                          title={cellToText(cell)}
                        >
                          {cellToText(cell)}
                        </td>
                      ))}
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
