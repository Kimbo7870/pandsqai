import { useEffect, useMemo, useRef, useState } from "react";
import type { MultiDatasetListItem } from "../lib/multifileTypes";
import type {
  CellLike,
  MultiOpsRequest,
  OpsCmp,
  OpsMergeHow,
  OpsTable,
} from "../lib/multifileTypes";
import { runMultiOps } from "../lib/multifileApi";

type EditorMode = "overlay" | "split" | "full";

type FilterRow = {
  id: string;
  column: string;
  cmp: OpsCmp;
  valueType: "string" | "number" | "boolean" | "null";
  valueText: string;
  valueBool: boolean;
};

function clampInt(v: number, lo: number, hi: number) {
  return Math.max(lo, Math.min(hi, v));
}

function parseCommaList(s: string): string[] {
  return s
    .split(",")
    .map((x) => x.trim())
    .filter((x) => x.length > 0);
}

function mkId() {
  return Math.random().toString(16).slice(2);
}

function toCellLike(f: FilterRow): CellLike {
  if (f.valueType === "null") return null;
  if (f.valueType === "boolean") return f.valueBool;

  if (f.valueType === "number") {
    const n = Number(f.valueText);
    if (Number.isNaN(n)) return null;
    return n;
  }

  return f.valueText;
}

export function MultiOpsEditor(props: {
  datasets: MultiDatasetListItem[];
  mode: EditorMode;
  onClose: () => void;
}) {
  const { datasets, mode, onClose } = props;

  const tableMapping = useMemo(() => {
    const lines: Array<{ t: OpsTable; name: string }> = [];
    if (datasets[0]) lines.push({ t: "t1", name: datasets[0].display_name });
    if (datasets[1]) lines.push({ t: "t2", name: datasets[1].display_name });
    if (datasets[2]) lines.push({ t: "t3", name: datasets[2].display_name });
    return lines;
  }, [datasets]);

  const [baseTable, setBaseTable] = useState<OpsTable>("t1");
  const [doMerge, setDoMerge] = useState<boolean>(datasets.length >= 2);
  const [rightTable, setRightTable] = useState<OpsTable>("t2");
  const [how, setHow] = useState<OpsMergeHow>("inner");
  const [leftOn, setLeftOn] = useState<string>("id");
  const [rightOn, setRightOn] = useState<string>("id");

  const [selectCols, setSelectCols] = useState<string>("");
  const [limitN, setLimitN] = useState<number>(200);

  const [filters, setFilters] = useState<FilterRow[]>([]);

  const [jsonMode, setJsonMode] = useState<boolean>(false);
  const [jsonText, setJsonText] = useState<string>("");

  const [loading, setLoading] = useState<boolean>(false);
  const [err, setErr] = useState<string>("");
  const [result, setResult] = useState<{
    columns: string[];
    rows: unknown[][];
    truncated: boolean;
    note: string | null;
  } | null>(null);

  const abortRef = useRef<AbortController | null>(null);

  useEffect(() => {
    if (datasets.length < 2) setDoMerge(false);

    // keep right table reasonable when only 1 dataset exists
    if (datasets.length < 2 && rightTable !== "t1") setRightTable("t1");
    if (datasets.length === 2 && rightTable === "t3") setRightTable("t2");
  }, [datasets.length, rightTable]);

  const builtPipeline: MultiOpsRequest = useMemo(() => {
    const steps: MultiOpsRequest["steps"] = [{ op: "source", table: baseTable }];

    const sel = selectCols.trim();
    if (sel.length > 0) {
      steps.push({ op: "select", columns: parseCommaList(sel) });
    }

    if (filters.length > 0) {
      steps.push({
        op: "filter",
        conditions: filters.map((f) => ({
          column: f.column.trim(),
          cmp: f.cmp,
          value: toCellLike(f),
        })),
      });
    }

    if (doMerge) {
      steps.push({
        op: "merge",
        right_table: rightTable,
        how,
        left_on: parseCommaList(leftOn),
        right_on: parseCommaList(rightOn),
      });
    }

    steps.push({ op: "limit", n: clampInt(limitN, 1, 100000) });

    return { steps, max_cells: 20000 };
  }, [baseTable, selectCols, filters, doMerge, rightTable, how, leftOn, rightOn, limitN]);

  useEffect(() => {
    if (!jsonMode) setJsonText(JSON.stringify(builtPipeline, null, 2));
  }, [builtPipeline, jsonMode]);

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") onClose();
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [onClose]);

  useEffect(() => {
    if (mode !== "overlay") return;
    const prev = document.body.style.overflow;
    document.body.style.overflow = "hidden";
    return () => {
      document.body.style.overflow = prev;
    };
  }, [mode]);

  async function onRun() {
    setErr("");
    setResult(null);

    let payload: MultiOpsRequest;
    if (jsonMode) {
      try {
        payload = JSON.parse(jsonText) as MultiOpsRequest;
      } catch {
        setErr("JSON parse error (your pipeline JSON is invalid).");
        return;
      }
    } else {
      payload = builtPipeline;
    }

    abortRef.current?.abort();
    const ac = new AbortController();
    abortRef.current = ac;

    setLoading(true);
    try {
      const res = await runMultiOps(payload, ac.signal);
      setResult(res);
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      setErr(msg);
    } finally {
      setLoading(false);
    }
  }

  const content = (
    <div className="w-full flex flex-col gap-3">
      <div className="flex items-center justify-between gap-2">
        <div className="text-lg font-semibold text-gray-900">Ops editor</div>
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

      <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
        <div className="p-3 rounded border border-gray-300 bg-white">
          <div className="text-sm font-semibold mb-2 text-gray-900">Pipeline builder</div>

          <div className="flex flex-col gap-2">
            <label className="text-xs text-gray-600">Base table</label>
            <select
              className="px-2 py-1 rounded bg-white border border-gray-300 text-gray-900"
              value={baseTable}
              onChange={(e) => setBaseTable(e.target.value as OpsTable)}
              disabled={datasets.length === 0}
            >
              <option value="t1" disabled={datasets.length < 1}>
                t1
              </option>
              <option value="t2" disabled={datasets.length < 2}>
                t2
              </option>
              <option value="t3" disabled={datasets.length < 3}>
                t3
              </option>
            </select>

            <label className="text-xs text-gray-600 mt-2">
              Select columns (comma-separated, optional)
            </label>
            <input
              className="px-2 py-1 rounded bg-white border border-gray-300 text-gray-900"
              value={selectCols}
              onChange={(e) => setSelectCols(e.target.value)}
              placeholder="e.g. id, amount, city"
            />

            <div className="flex items-center justify-between mt-3">
              <div className="text-xs text-gray-600">Filters (optional)</div>
              <button
                className="px-2 py-1 rounded bg-gray-700 text-white hover:bg-gray-800 text-xs"
                type="button"
                onClick={() =>
                  setFilters((prev) => [
                    ...prev,
                    {
                      id: mkId(),
                      column: "",
                      cmp: "==",
                      valueType: "string",
                      valueText: "",
                      valueBool: false,
                    },
                  ])
                }
              >
                + Add filter
              </button>
            </div>

            {filters.length > 0 && (
              <div className="flex flex-col gap-2">
                {filters.map((f) => (
                  <div key={f.id} className="grid grid-cols-12 gap-2 items-center">
                    <input
                      className="col-span-4 px-2 py-1 rounded bg-white border border-gray-300 text-gray-900"
                      placeholder="column"
                      value={f.column}
                      onChange={(e) =>
                        setFilters((prev) =>
                          prev.map((x) => (x.id === f.id ? { ...x, column: e.target.value } : x))
                        )
                      }
                    />

                    <select
                      className="col-span-2 px-2 py-1 rounded bg-white border border-gray-300 text-gray-900"
                      value={f.cmp}
                      onChange={(e) =>
                        setFilters((prev) =>
                          prev.map((x) => (x.id === f.id ? { ...x, cmp: e.target.value as OpsCmp } : x))
                        )
                      }
                    >
                      <option value="==">==</option>
                      <option value="!=">!=</option>
                      <option value="<">&lt;</option>
                      <option value="<=">&lt;=</option>
                      <option value=">">&gt;</option>
                      <option value=">=">&gt;=</option>
                    </select>

                    <select
                      className="col-span-3 px-2 py-1 rounded bg-white border border-gray-300 text-gray-900"
                      value={f.valueType}
                      onChange={(e) =>
                        setFilters((prev) =>
                          prev.map((x) =>
                            x.id === f.id ? { ...x, valueType: e.target.value as FilterRow["valueType"] } : x
                          )
                        )
                      }
                    >
                      <option value="string">string</option>
                      <option value="number">number</option>
                      <option value="boolean">boolean</option>
                      <option value="null">null</option>
                    </select>

                    <div className="col-span-2">
                      {f.valueType === "boolean" ? (
                        <label className="flex items-center gap-2 text-xs text-gray-700">
                          <input
                            type="checkbox"
                            checked={f.valueBool}
                            onChange={(e) =>
                              setFilters((prev) =>
                                prev.map((x) => (x.id === f.id ? { ...x, valueBool: e.target.checked } : x))
                              )
                            }
                          />
                          true
                        </label>
                      ) : f.valueType === "null" ? (
                        <div className="text-xs text-gray-600 px-2 py-1">null</div>
                      ) : (
                        <input
                          className="w-full px-2 py-1 rounded bg-white border border-gray-300 text-gray-900"
                          placeholder="value"
                          value={f.valueText}
                          onChange={(e) =>
                            setFilters((prev) =>
                              prev.map((x) => (x.id === f.id ? { ...x, valueText: e.target.value } : x))
                            )
                          }
                        />
                      )}
                    </div>

                    <button
                      className="col-span-1 px-2 py-1 rounded bg-white border border-gray-300 text-gray-700 hover:bg-gray-50 text-xs"
                      type="button"
                      onClick={() => setFilters((prev) => prev.filter((x) => x.id !== f.id))}
                    >
                      ✕
                    </button>
                  </div>
                ))}
              </div>
            )}

            <div className="flex items-center gap-2 mt-3">
              <input
                type="checkbox"
                checked={doMerge}
                disabled={datasets.length < 2}
                onChange={(e) => setDoMerge(e.target.checked)}
              />
              <span className="text-xs text-gray-700">Merge</span>
            </div>

            {doMerge && (
              <div className="grid grid-cols-1 gap-2">
                <div className="grid grid-cols-2 gap-2">
                  <div>
                    <label className="text-xs text-gray-600">Right table</label>
                    <select
                      className="w-full px-2 py-1 rounded bg-white border border-gray-300 text-gray-900"
                      value={rightTable}
                      onChange={(e) => setRightTable(e.target.value as OpsTable)}
                    >
                      <option value="t1" disabled={datasets.length < 1}>
                        t1
                      </option>
                      <option value="t2" disabled={datasets.length < 2}>
                        t2
                      </option>
                      <option value="t3" disabled={datasets.length < 3}>
                        t3
                      </option>
                    </select>
                  </div>

                  <div>
                    <label className="text-xs text-gray-600">How</label>
                    <select
                      className="w-full px-2 py-1 rounded bg-white border border-gray-300 text-gray-900"
                      value={how}
                      onChange={(e) => setHow(e.target.value as OpsMergeHow)}
                    >
                      <option value="inner">inner</option>
                      <option value="left">left</option>
                      <option value="right">right</option>
                      <option value="outer">outer</option>
                    </select>
                  </div>
                </div>

                <label className="text-xs text-gray-600">Left keys (comma-separated)</label>
                <input
                  className="px-2 py-1 rounded bg-white border border-gray-300 text-gray-900"
                  value={leftOn}
                  onChange={(e) => setLeftOn(e.target.value)}
                  placeholder="e.g. id"
                />

                <label className="text-xs text-gray-600">Right keys (comma-separated)</label>
                <input
                  className="px-2 py-1 rounded bg-white border border-gray-300 text-gray-900"
                  value={rightOn}
                  onChange={(e) => setRightOn(e.target.value)}
                  placeholder="e.g. id"
                />
              </div>
            )}

            <label className="text-xs text-gray-600 mt-3">Limit</label>
            <input
              type="number"
              className="px-2 py-1 rounded bg-white border border-gray-300 text-gray-900"
              value={limitN}
              onChange={(e) => setLimitN(Number(e.target.value))}
              min={1}
              max={100000}
            />

            <div className="flex items-center gap-2 mt-3">
              <button
                className="px-3 py-1 rounded bg-gray-700 text-white hover:bg-gray-800 disabled:opacity-50"
                onClick={onRun}
                disabled={loading || datasets.length === 0}
                type="button"
              >
                {loading ? "Running..." : "Run"}
              </button>

              <button
                className="px-3 py-1 rounded bg-white border border-gray-300 text-gray-900 hover:bg-gray-50"
                type="button"
                onClick={() => setJsonMode((v) => !v)}
              >
                {jsonMode ? "Use Builder" : "Edit JSON"}
              </button>
            </div>

            {err && <div className="text-sm text-red-600 mt-2 whitespace-pre-wrap">{err}</div>}
          </div>
        </div>

        <div className="p-3 rounded border border-gray-300 bg-white flex flex-col gap-2">
          <div className="text-sm font-semibold text-gray-900">Pipeline JSON</div>
          <textarea
            className="w-full flex-1 min-h-[220px] px-2 py-1 rounded bg-white border border-gray-300 font-mono text-xs text-gray-900"
            value={jsonText}
            readOnly={!jsonMode}
            onChange={(e) => setJsonText(e.target.value)}
          />
        </div>
      </div>

      <div className="p-3 rounded border border-gray-300 bg-white">
        <div className="flex items-center justify-between">
          <div className="text-sm font-semibold text-gray-900">Results</div>
          {result?.truncated && (
            <div className="text-xs text-amber-700">{result.note ?? "Truncated result"}</div>
          )}
        </div>

        {!result && !loading && <div className="text-sm text-gray-600 mt-2">Run to see results.</div>}

        {result && (
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
        )}
      </div>
    </div>
  );

  if (mode === "overlay") {
    return (
      <div
        className="fixed inset-0 z-50 bg-black/50 flex items-center justify-center p-4"
        onMouseDown={(e) => {
          if (e.target === e.currentTarget) onClose();
        }}
      >
        {/* ✅ KEY FIX: allow modal to scroll instead of cutting off */}
        <div
          className="w-full max-w-5xl max-h-[85vh] rounded bg-gray-50 border border-gray-300 p-4 overflow-auto"
          onMouseDown={(e) => e.stopPropagation()}
        >
          {content}
        </div>
      </div>
    );
  }

  // ✅ KEY FIX: non-overlay should be scrollable too (full/split containers vary)
  return (
    <div className="w-full rounded bg-gray-50 border border-gray-300 p-4 overflow-auto">
      {content}
    </div>
  );
}
