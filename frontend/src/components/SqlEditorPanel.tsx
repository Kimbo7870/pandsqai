import { useEffect, useRef, useState } from "react";

import { getDataset } from "../lib/api";
import type { Cell, UploadInfo } from "../lib/types";

interface Props {
  dataset_id: string | null;
  query: string;
  onQueryChange: (nextQuery: string) => void;
}

type SqlResult = {
  columns: string[];
  rows: Array<Array<Cell>>;
};

let sqlJsLoader: Promise<SqlJsStatic> | null = null;

function loadScript(src: string): Promise<void> {
  return new Promise((resolve, reject) => {
    const existing = document.querySelector(`script[src="${src}"]`);
    if (existing) {
      resolve();
      return;
    }
    const s = document.createElement("script");
    s.src = src;
    s.async = true;
    s.onload = () => resolve();
    s.onerror = () => reject(new Error(`Failed to load script: ${src}`));
    document.head.appendChild(s);
  });
}

async function ensureSqlJs(indexUrl: string): Promise<SqlJsStatic> {
  if (sqlJsLoader) return sqlJsLoader;

  sqlJsLoader = (async () => {
    await loadScript(`${indexUrl}sql-wasm.js`);

    const init = window.initSqlJs;
    if (!init) {
      throw new Error("sql.js initSqlJs not found on window.");
    }

    const SQL: SqlJsStatic = await init({ locateFile: (f: string) => `${indexUrl}${f}` });
    return SQL;
  })();

  return sqlJsLoader;
}

function quoteIdent(name: string): string {
  const safe = name.replaceAll('"', '""');
  return `"${safe}"`;
}

function inferSqlType(values: Array<Cell>): "INTEGER" | "REAL" | "TEXT" {
  for (const v of values) {
    if (v === null || v === undefined) continue;
    if (typeof v === "boolean") return "INTEGER";
    if (typeof v === "number") {
      return Number.isInteger(v) ? "INTEGER" : "REAL";
    }
    return "TEXT";
  }
  return "TEXT";
}

function normalizeCell(v: Cell): Cell {
  if (v === undefined) return null;
  if (typeof v === "boolean") return v ? 1 : 0;
  return v;
}

export default function SqlEditorPanel({ dataset_id, query, onQueryChange }: Props) {
  const [info, setInfo] = useState<UploadInfo | null>(null);
  const [loading, setLoading] = useState(false);
  const [loadErr, setLoadErr] = useState<string>("");
  const [runtimeErr, setRuntimeErr] = useState<string>("");

  const [result, setResult] = useState<SqlResult | null>(null);
  const [runErr, setRunErr] = useState<string>("");
  const [runNote, setRunNote] = useState<string>("");
  const [sqlReady, setSqlReady] = useState(false);

  const dbRef = useRef<SqlJsDatabase | null>(null);

  useEffect(() => {
    let cancelled = false;
    async function load() {
      if (!dataset_id) return;
      setLoading(true);
      setLoadErr("");
      setRuntimeErr("");
      setInfo(null);
      setSqlReady(false);
      setResult(null);
      setRunErr("");
      setRunNote("");

      try {
        const ds = await getDataset(dataset_id);
        if (cancelled) return;
        setInfo(ds);

        // Pin a specific version for stability.
        const indexUrl = "https://cdn.jsdelivr.net/npm/sql.js@1.10.3/dist/";
        const SQL = await ensureSqlJs(indexUrl);

        const db = new SQL.Database();
        dbRef.current = db;

        // Build a simple in-memory table from the 50-row preview.
        const colDefs = ds.columns
          .map((c) => {
            const t = inferSqlType(ds.sample.map((r) => r[c] as Cell));
            return `${quoteIdent(c)} ${t}`;
          })
          .join(", ");
        db.run(`CREATE TABLE data (${colDefs});`);

        const placeholders = ds.columns.map(() => "?").join(",");
        const insertSql = `INSERT INTO data (${ds.columns
          .map((c) => quoteIdent(c))
          .join(",")}) VALUES (${placeholders});`;
        const stmt = db.prepare(insertSql);
        for (const row of ds.sample) {
          const values = ds.columns.map((c) => normalizeCell(row[c] as Cell));
          stmt.run(values);
        }
        stmt.free();

        setSqlReady(true);
        setRunNote("Loaded the preview rows into an in-browser SQLite table named: data");
      } catch (err: unknown) {
        const message = err instanceof Error ? err.message : String(err);
        if (!cancelled) {
          const m = message || "Failed to load dataset";
          if (m.toLowerCase().includes("sql") || m.toLowerCase().includes("script")) {
            setRuntimeErr(m);
          } else {
            setLoadErr(m);
          }
        }
      } finally {
        if (!cancelled) setLoading(false);
      }
    }
    void load();
    return () => {
      cancelled = true;
      try {
        dbRef.current?.close?.();
      } catch {
        // ignore
      }
      dbRef.current = null;
    };
  }, [dataset_id]);

  function onRun() {
    setRunErr("");
    setRunNote("");
    setResult(null);

    if (!dataset_id) {
      setRunErr("Upload or load a dataset first.");
      return;
    }
    if (!sqlReady || !dbRef.current) {
      setRunErr("SQL runtime not ready yet.");
      return;
    }

    try {
      const res = dbRef.current.exec(query);
      if (!res || res.length === 0) {
        setRunNote("Query executed (no rows returned).");
        return;
      }
      const first = res[0] as { columns: string[]; values: Array<Array<Cell>> };
      const rows = (first.values ?? []).slice(0, 200);
      setResult({ columns: first.columns, rows });
      if ((first.values ?? []).length > 200) {
        setRunNote("Showing first 200 rows.");
      }
    } catch (err: unknown) {
      const message = err instanceof Error ? err.message : String(err);
      setRunErr(message || "Query failed");
    }
  }

  if (!dataset_id) {
    return <p className="text-gray-700">Upload or load a dataset first.</p>;
  }

  return (
    <div className="space-y-4">
      {loading && <p className="text-gray-600">Loading dataset preview...</p>}
      {loadErr && <p className="text-gray-700">{loadErr}</p>}
      {runtimeErr && (
        <p className="text-gray-700">Failed to load SQL runtime in the browser: {runtimeErr}</p>
      )}

      {dataset_id && info && (
        <>
          <div className="text-sm text-gray-700">
            <div>
              <b>Table:</b> data
            </div>
            <div className="mt-1">
              <b>Columns:</b> {info.columns.join(", ")}
            </div>
            <div className="mt-1 text-gray-600">
              Uses only the first {info.sample.length} rows (the upload preview) for now.
            </div>
          </div>

          <div className="space-y-2">
            <label className="block text-sm font-semibold">SQL query</label>
            <textarea
              value={query}
              onChange={(e) => onQueryChange(e.target.value)}
              className="w-full h-40 p-3 font-mono text-sm border border-gray-300 rounded bg-white"
              spellCheck={false}
            />
            <div className="flex items-center gap-2">
              <button
                onClick={onRun}
                disabled={!sqlReady}
                className="px-4 py-2 bg-gray-600 text-white rounded hover:bg-gray-700 active:bg-gray-800 transition-colors disabled:bg-gray-400"
              >
                Run
              </button>
              {!sqlReady && <span className="text-sm text-gray-600">Preparing SQL…</span>}
            </div>
          </div>

          {(runErr || runNote) && (
            <div className="text-sm">
              {runErr && <p className="text-gray-700">{runErr}</p>}
              {runNote && <p className="text-gray-600">{runNote}</p>}
            </div>
          )}

          {result && (
            <div className="space-y-2">
              <div className="text-sm text-gray-600">
                Returned {result.rows.length.toLocaleString()} row(s).
              </div>
              <div className="overflow-auto border border-gray-300 rounded">
                <table className="min-w-full text-sm">
                  <thead>
                    <tr className="bg-gray-100">
                      {result.columns.map((c) => (
                        <th key={c} className="px-3 py-2 text-left border-b border-gray-300">
                          {c}
                        </th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {result.rows.map((row, i) => (
                      <tr key={i} className="odd:bg-gray-50">
                        {row.map((v, j) => (
                          <td key={j} className="px-3 py-2 border-b border-gray-300">
                            {String(v ?? "")}
                          </td>
                        ))}
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          )}
        </>
      )}
    </div>
  );
}
