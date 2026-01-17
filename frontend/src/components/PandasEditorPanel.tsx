import { useEffect, useState } from "react";

import { getDataset } from "../lib/api";
import type { Cell, UploadInfo } from "../lib/types";

interface Props {
  dataset_id: string | null;
  code: string;
  onCodeChange: (nextCode: string) => void;
}

type RuntimeStatus = "idle" | "loading" | "ready" | "error";

type PandasRunResult = {
  stdout: string;
  error: string;
  df_head_json: string;
  result_repr: string;
};

let pyodideLoader: Promise<Pyodide> | null = null;

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

function sanitizeRows(columns: string[], rows: Array<Record<string, Cell>>) {
  return rows.map((r) => {
    const out: Record<string, Cell> = {};
    for (const c of columns) {
      const v = r[c];
      out[c] = v === undefined ? null : v;
    }
    return out;
  });
}

async function ensurePyodide(indexUrl: string): Promise<Pyodide> {
  if (pyodideLoader) return pyodideLoader;

  pyodideLoader = (async () => {
    // Load the loader script into the page.
    await loadScript(`${indexUrl}pyodide.js`);

    const loader = window.loadPyodide;
    if (!loader) {
      throw new Error("Pyodide loader not found on window.");
    }

    const py = await loader({ indexURL: indexUrl });
    await py.loadPackage("pandas");
    return py;
  })();

  return pyodideLoader;
}

export default function PandasEditorPanel({ dataset_id, code, onCodeChange }: Props) {
  const [info, setInfo] = useState<UploadInfo | null>(null);
  const [loading, setLoading] = useState(false);
  const [loadErr, setLoadErr] = useState<string>("");

  const [runtimeStatus, setRuntimeStatus] = useState<RuntimeStatus>("idle");
  const [runtimeErr, setRuntimeErr] = useState<string>("");

  const [stdout, setStdout] = useState<string>("");
  const [error, setError] = useState<string>("");
  const [resultRepr, setResultRepr] = useState<string>("");
  // const [dfHeadRows, setDfHeadRows] = useState<Array<Record<string, Cell>>>([]);
  const [running, setRunning] = useState(false);

  // Load dataset preview
  useEffect(() => {
    let cancelled = false;
    async function load() {
      if (!dataset_id) return;
      setLoading(true);
      setLoadErr("");
      setInfo(null);
      setStdout("");
      setError("");
      setResultRepr("");

      try {
        const ds = await getDataset(dataset_id);
        if (cancelled) return;
        setInfo(ds);
      } catch (err: unknown) {
        const message = err instanceof Error ? err.message : String(err);
        if (!cancelled) setLoadErr(message || "Failed to load dataset");
      } finally {
        if (!cancelled) setLoading(false);
      }
    }

    void load();
    return () => {
      cancelled = true;
    };
  }, [dataset_id]);

  async function onLoadRuntime() {
    setRuntimeErr("");
    setRuntimeStatus("loading");

    try {
      // Pin a specific version for stability.
      const indexUrl = "https://cdn.jsdelivr.net/pyodide/v0.25.1/full/";
      await ensurePyodide(indexUrl);
      setRuntimeStatus("ready");
    } catch (err: unknown) {
      const message = err instanceof Error ? err.message : String(err);
      setRuntimeErr(message || "Failed to load Python runtime");
      setRuntimeStatus("error");
    }
  }

  async function onRun() {
    setStdout("");
    setError("");
    setResultRepr("");

    if (!dataset_id) {
      setError("Upload or load a dataset first.");
      return;
    }
    if (!info) {
      setError("Dataset not ready yet.");
      return;
    }
    if (runtimeStatus !== "ready") {
      setError("Load the Python runtime first.");
      return;
    }

    setRunning(true);
    try {
      const indexUrl = "https://cdn.jsdelivr.net/pyodide/v0.25.1/full/";
      const py = await ensurePyodide(indexUrl);

      const safeRows = sanitizeRows(info.columns, info.sample);
      const rowsJson = JSON.stringify(safeRows);
      py.globals.set("rows_json", rowsJson);

      const pyCode = `import json, pandas as pd, sys, io, traceback\n\nrows = json.loads(rows_json)\ndf = pd.DataFrame(rows)\n\n_stdout = io.StringIO()\nsys.stdout = _stdout\n_err = ''\n_result_repr = ''\n\ntry:\n${code
        .split("\n")
        .map((line) => `    ${line}`)
        .join("\n")}\nexcept Exception:\n    _err = traceback.format_exc()\n\n_out = _stdout.getvalue()\n\n# Optional user-facing "result"\ntry:\n    if 'result' in globals():\n        _result_repr = str(globals()['result'])\nexcept Exception:\n    _result_repr = '<unprintable result>'\n\n# Option A: do NOT compute df.head automatically\njson.dumps({\n  'stdout': _out,\n  'error': _err,\n  'df_head_json': '[]',\n  'result_repr': _result_repr,\n})\n`;

      const outRaw = await py.runPythonAsync(pyCode);
      const outStr = typeof outRaw === "string" ? outRaw : String(outRaw);
      const parsed = JSON.parse(outStr) as PandasRunResult;

      setStdout(parsed.stdout || "");
      setError(parsed.error || "");
      setResultRepr(parsed.result_repr || "");

    } catch (err: unknown) {
      const message = err instanceof Error ? err.message : String(err);
      setError(message || "Run failed");
    } finally {
      setRunning(false);
    }
  }

  if (!dataset_id) {
    return <p className="text-gray-700">Upload or load a dataset first.</p>;
  }

  return (
    <div className="space-y-4">
      {loading && <p className="text-gray-600">Loading dataset preview...</p>}
      {loadErr && <p className="text-gray-700">{loadErr}</p>}

      {dataset_id && info && (
        <>
          <div className="text-sm text-gray-700">
            <div>
              Uses only the first {info.sample.length} rows (the upload preview) for now.
            </div>
          </div>

          <div className="flex items-center gap-2">
            <button
              onClick={onLoadRuntime}
              disabled={runtimeStatus === "loading" || runtimeStatus === "ready"}
              className="px-4 py-2 bg-gray-600 text-white rounded hover:bg-gray-700 active:bg-gray-800 transition-colors disabled:bg-gray-400"
            >
              {runtimeStatus === "ready"
                ? "Python runtime loaded"
                : runtimeStatus === "loading"
                  ? "Loading Python..."
                  : "Load Python runtime"}
            </button>
            {runtimeErr && <span className="text-sm text-gray-700">{runtimeErr}</span>}
            {runtimeStatus === "idle" && (
              <span className="text-sm text-gray-600">(Uses Pyodide in the browser)</span>
            )}
          </div>

          <div className="space-y-2">
            <label className="block text-sm font-semibold">Python code</label>
            <textarea
              value={code}
              onChange={(e) => onCodeChange(e.target.value)}
              className="w-full h-56 p-3 font-mono text-sm border border-gray-300 rounded bg-white"
              spellCheck={false}
            />
            <div className="flex items-center gap-2">
              <button
                onClick={onRun}
                disabled={running}
                className="px-4 py-2 bg-gray-600 text-white rounded hover:bg-gray-700 active:bg-gray-800 transition-colors disabled:bg-gray-400"
              >
                {running ? "Running..." : "Run"}
              </button>
              <span className="text-sm text-gray-600">df is preloaded as a pandas DataFrame</span>
            </div>
          </div>

          {(stdout || error || resultRepr) && (
            <div className="space-y-2">
              {error && (
                <div className="border border-gray-300 rounded p-3 bg-gray-50">
                  <div className="text-sm font-semibold">Error</div>
                  <pre className="text-sm whitespace-pre-wrap">{error}</pre>
                </div>
              )}
              {stdout && (
                <div className="border border-gray-300 rounded p-3 bg-gray-50">
                  <div className="text-sm font-semibold">Stdout</div>
                  <pre className="text-sm whitespace-pre-wrap">{stdout}</pre>
                </div>
              )}
              {resultRepr && (
                <div className="border border-gray-300 rounded p-3 bg-gray-50">
                  <div className="text-sm font-semibold">result</div>
                  <pre className="text-sm whitespace-pre-wrap">{resultRepr}</pre>
                </div>
              )}
            </div>
          )}
        </>
      )}
    </div>
  );
}
