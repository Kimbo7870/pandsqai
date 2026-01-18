import { useEffect, useMemo, useRef, useState } from "react";
import type { MultiDatasetListItem } from "../lib/multifileTypes";
import { runMultiSql } from "../lib/multifileApi";

export type MultiEditorMode = "overlay" | "split" | "full";

const SQL_QUERY_KEY = "pandsqai.multifile.sqlQuery.v1";

function defaultQueryForCount(count: number) {
  if (count <= 0) return "SELECT 1;";
  return "SELECT * FROM t1 LIMIT 50;";
}

function readInitialQuery(count: number): string {
  try {
    const raw = localStorage.getItem(SQL_QUERY_KEY);
    if (raw && raw.trim().length > 0) return raw;
  } catch {
    // ignore
  }
  return defaultQueryForCount(count);
}

function writeQuery(q: string) {
  try {
    localStorage.setItem(SQL_QUERY_KEY, q);
  } catch {
    // ignore
  }
}

function isTabularResult(x: unknown): x is {
  columns: string[];
  rows: unknown[][];
  truncated?: boolean;
  note?: string | null;
} {
  if (!x || typeof x !== "object") return false;
  const obj = x as { columns?: unknown; rows?: unknown };
  if (!Array.isArray(obj.columns) || !Array.isArray(obj.rows)) return false;
  return true;
}

export default function MultiSqlEditor(props: {
  open: boolean;
  mode: MultiEditorMode;
  datasets: MultiDatasetListItem[];
  onClose: () => void;
}) {
  const { open, mode, datasets, onClose } = props;

  const tableMapping = useMemo(() => {
    const lines: Array<{ t: "t1" | "t2" | "t3"; name: string }> = [];
    if (datasets[0]) lines.push({ t: "t1", name: datasets[0].display_name });
    if (datasets[1]) lines.push({ t: "t2", name: datasets[1].display_name });
    if (datasets[2]) lines.push({ t: "t3", name: datasets[2].display_name });
    return lines;
  }, [datasets]);

  const [query, setQuery] = useState<string>(() => readInitialQuery(datasets.length));
  const [loading, setLoading] = useState<boolean>(false);
  const [err, setErr] = useState<string>("");
  const [result, setResult] = useState<unknown>(null);

  const textareaRef = useRef<HTMLTextAreaElement | null>(null);

  useEffect(() => {
    if (query.trim().length > 0) return;
    setQuery(defaultQueryForCount(datasets.length));
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [datasets.length]);

  useEffect(() => {
    writeQuery(query);
  }, [query]);

  useEffect(() => {
    if (mode !== "overlay" || !open) return;
    const prev = document.body.style.overflow;
    document.body.style.overflow = "hidden";
    return () => {
      document.body.style.overflow = prev;
    };
  }, [mode, open]);

  useEffect(() => {
    if (mode === "overlay" && !open) return;
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") onClose();
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [mode, open, onClose]);

  async function onRun() {
    setErr("");
    setResult(null);

    setLoading(true);
    try {
      // ✅ your repo expects a string here
      const res = await runMultiSql(query);
      setResult(res);
    } catch (e) {
      setErr(e instanceof Error ? e.message : String(e));
    } finally {
      setLoading(false);
    }
  }

  const content = (
    <div className="h-full w-full flex flex-col gap-3">
      <div className="flex items-center justify-between gap-2">
        <div className="text-lg font-semibold text-gray-900">SQL editor</div>
        <button
          className="px-3 py-1 rounded bg-gray-700 text-white hover:bg-gray-800"
          onClick={onClose}
          type="button"
        >
          Close
        </button>
      </div>

      <div className="text-sm text-gray-600">
        {tableMapping.map((m) => (
          <div key={m.t}>
            <span className="font-mono">{m.t}</span> = {m.name}
          </div>
        ))}
      </div>

      <div className="grid grid-cols-1 gap-2">
        <div className="flex items-center justify-between gap-2">
          <div className="text-sm font-semibold text-gray-900">Query</div>
          <button
            type="button"
            onClick={onRun}
            disabled={loading || datasets.length === 0}
            className="px-3 py-1 rounded bg-gray-700 text-white hover:bg-gray-800 disabled:opacity-50"
          >
            {loading ? "Running..." : "Run"}
          </button>
        </div>

        <textarea
          ref={textareaRef}
          className="w-full min-h-40 px-3 py-2 rounded border border-gray-300 bg-white font-mono text-sm text-gray-900"
          value={query}
          onChange={(e) => setQuery(e.target.value)}
          autoFocus={mode !== "split"}
          spellCheck={false}
          onKeyDown={(e) => {
            // ✅ prevents any parent key handlers from stealing focus
            e.stopPropagation();
          }}
          onMouseDown={(e) => {
            // ✅ prevents overlay backdrop from interfering
            e.stopPropagation();
          }}
        />

        {err && <div className="text-sm text-red-600 whitespace-pre-wrap">{err}</div>}
      </div>

      <div className="p-3 rounded border border-gray-300 bg-white">
        <div className="text-sm font-semibold text-gray-900">Results</div>

        {!result && !loading && <div className="text-sm text-gray-600 mt-2">Run to see results.</div>}

        {result && isTabularResult(result) ? (
          <div className="mt-2 overflow-auto max-h-[420px] border border-gray-200 rounded">
            <table className="min-w-full text-sm">
              <thead className="bg-gray-50">
                <tr>
                  {result.columns.map((c) => (
                    <th key={c} className="text-left px-2 py-2 border-b border-gray-200">
                      {c}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {result.rows.map((row, i) => (
                  <tr key={i} className="odd:bg-white even:bg-gray-50">
                    {row.map((cell, j) => (
                      <td key={j} className="px-2 py-2 border-b border-gray-200">
                        {cell === null ? "null" : String(cell)}
                      </td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        ) : (
          <pre className="mt-2 text-xs bg-gray-50 border border-gray-200 rounded p-2 overflow-auto max-h-[420px]">
            {JSON.stringify(result, null, 2)}
          </pre>
        )}
      </div>
    </div>
  );

  if (mode === "overlay") {
    if (!open) return null;
    return (
      <div
        className="fixed inset-0 z-50 bg-black/50 flex items-center justify-center p-4"
        onMouseDown={() => onClose()}
      >
        <div
          className="w-full max-w-5xl h-[85vh] rounded bg-gray-50 border border-gray-300 p-4 overflow-hidden"
          onMouseDown={(e) => e.stopPropagation()}
        >
          {content}
        </div>
      </div>
    );
  }

  if (mode === "full") {
    return (
      <div className="min-h-screen bg-gray-50">
        <div className="max-w-6xl mx-auto p-6">
          <div className="h-[calc(100vh-48px)] rounded bg-gray-50 border border-gray-300 p-4 overflow-hidden">
            {content}
          </div>
        </div>
      </div>
    );
  }

  return (
    <div className="h-full w-full rounded bg-gray-50 border border-gray-300 p-4 overflow-hidden">
      {content}
    </div>
  );
}
