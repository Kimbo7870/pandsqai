import { useEffect, useMemo, useState } from "react";
import { Link } from "react-router-dom";
import { getMultiCurrent } from "../lib/multifileApi";
import type { MultiDatasetListItem, PreviewDims } from "../lib/multifileTypes";
import DatasetViewport from "./DatasetViewport";
import MultiSqlEditor, { type MultiEditorMode } from "./MultiSqlEditor";
import { MultiOpsEditor } from "./MultiOpsEditor";

const PREVIEW_DIMS_KEY = "pandsqai.multifile.previewDims.v1";

type EditorKind = "sql" | "ops" | null;

function clampInt(n: number, lo: number, hi: number): number {
  if (Number.isNaN(n)) return lo;
  return Math.min(hi, Math.max(lo, Math.trunc(n)));
}

function readPreviewDims(defaultDims: PreviewDims): PreviewDims {
  try {
    const raw = localStorage.getItem(PREVIEW_DIMS_KEY);
    if (!raw) return defaultDims;
    const parsed: unknown = JSON.parse(raw);
    if (!parsed || typeof parsed !== "object") return defaultDims;
    const obj = parsed as { rows?: unknown; cols?: unknown };
    return {
      rows: clampInt(Number(obj.rows), 1, 100),
      cols: clampInt(Number(obj.cols), 1, 100),
    };
  } catch {
    return defaultDims;
  }
}

function toUserError(e: unknown): string {
  if (e instanceof Error) return e.message;
  return "Something went wrong";
}

function titleFor(i: number, d: MultiDatasetListItem): string {
  return `t${i + 1} = ${d.display_name}`;
}

function ModeSelector(props: {
  mode: MultiEditorMode;
  onChange: (m: MultiEditorMode) => void;
  disabled?: boolean;
}) {
  const { mode, onChange, disabled } = props;
  const base =
    "px-3 py-2 text-sm transition-colors disabled:opacity-60 disabled:cursor-not-allowed";
  const active = "bg-gray-700 text-white";
  const inactive = "bg-gray-200 text-gray-800 hover:bg-gray-300";

  return (
    <div className="inline-flex rounded overflow-hidden border border-gray-300">
      <button
        type="button"
        disabled={disabled}
        onClick={() => onChange("overlay")}
        className={`${base} ${mode === "overlay" ? active : inactive}`}
      >
        Overlay
      </button>
      <button
        type="button"
        disabled={disabled}
        onClick={() => onChange("split")}
        className={`${base} ${mode === "split" ? active : inactive}`}
      >
        Split
      </button>
      <button
        type="button"
        disabled={disabled}
        onClick={() => onChange("full")}
        className={`${base} ${mode === "full" ? active : inactive}`}
      >
        Full
      </button>
    </div>
  );
}

export default function MultiFileViewAll() {
  const defaultDims = useMemo<PreviewDims>(() => ({ rows: 20, cols: 20 }), []);
  const dims = useMemo(() => readPreviewDims(defaultDims), [defaultDims]);

  const [datasets, setDatasets] = useState<MultiDatasetListItem[]>([]);
  const [loading, setLoading] = useState<boolean>(true);
  const [err, setErr] = useState<string>("");

  const [editorKind, setEditorKind] = useState<EditorKind>(null);
  const [editorMode, setEditorMode] = useState<MultiEditorMode>("overlay");

  useEffect(() => {
    let cancelled = false;
    (async () => {
      setLoading(true);
      setErr("");
      try {
        const res = await getMultiCurrent();
        if (!cancelled) setDatasets(res.datasets);
      } catch (e) {
        if (!cancelled) setErr(toUserError(e));
      } finally {
        if (!cancelled) setLoading(false);
      }
    })();
    return () => {
      cancelled = true;
    };
  }, []);

  const count = datasets.length;

  // ✅ Only for View All + Split + editor open: stack vertically
  const stackForSplit = editorMode === "split" && editorKind !== null;

  const viewportBlock = loading ? (
    <p className="text-sm text-gray-600">Loading…</p>
  ) : count === 0 ? (
    <div className="border border-gray-300 rounded p-4 bg-white">
      <p className="text-sm text-gray-600">No datasets loaded. Go back and upload up to 3.</p>
    </div>
  ) : (
    <div className="border border-gray-300 rounded p-4 bg-white">
      {stackForSplit ? (
        // ✅ split+editor: always vertical stack
        <div className="flex flex-col gap-4 items-center">
          {datasets.slice(0, 3).map((d, i) => (
            <DatasetViewport
              key={d.dataset_id}
              dataset_id={d.dataset_id}
              title={titleFor(i, d)}
              viewRows={dims.rows}
              viewCols={dims.cols}
            />
          ))}
        </div>
      ) : count === 1 ? (
        <div className="flex justify-center">
          <DatasetViewport
            dataset_id={datasets[0].dataset_id}
            title={titleFor(0, datasets[0])}
            viewRows={dims.rows}
            viewCols={dims.cols}
          />
        </div>
      ) : count === 2 ? (
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-4 justify-items-center">
          {datasets.slice(0, 2).map((d, i) => (
            <DatasetViewport
              key={d.dataset_id}
              dataset_id={d.dataset_id}
              title={titleFor(i, d)}
              viewRows={dims.rows}
              viewCols={dims.cols}
            />
          ))}
        </div>
      ) : (
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-4 justify-items-center">
          <div className="lg:col-span-2 flex justify-center">
            <DatasetViewport
              dataset_id={datasets[0].dataset_id}
              title={titleFor(0, datasets[0])}
              viewRows={dims.rows}
              viewCols={dims.cols}
            />
          </div>
          <DatasetViewport
            dataset_id={datasets[1].dataset_id}
            title={titleFor(1, datasets[1])}
            viewRows={dims.rows}
            viewCols={dims.cols}
          />
          <DatasetViewport
            dataset_id={datasets[2].dataset_id}
            title={titleFor(2, datasets[2])}
            viewRows={dims.rows}
            viewCols={dims.cols}
          />
        </div>
      )}
    </div>
  );

  const page = (
    <div className="max-w-6xl mx-auto p-6 space-y-6">
      <div className="flex items-center justify-between gap-3">
        <div>
          <h1 className="text-2xl font-semibold text-gray-900">Multifile — View all</h1>
          <p className="text-sm text-gray-600 mt-1">
            Drag inside a window to pan. Using preview dims: {dims.rows}×{dims.cols}.
          </p>
        </div>

        <div className="flex flex-wrap gap-2 justify-end items-center">
          <ModeSelector mode={editorMode} onChange={setEditorMode} disabled={editorKind !== null} />

          <button
            onClick={() => setEditorKind("sql")}
            disabled={datasets.length === 0}
            className="px-4 py-2 bg-gray-600 text-white rounded hover:bg-gray-700 active:bg-gray-800 transition-colors disabled:bg-gray-400"
          >
            SQL editor
          </button>

          <button
            onClick={() => setEditorKind("ops")}
            disabled={datasets.length === 0}
            className="px-4 py-2 bg-gray-600 text-white rounded hover:bg-gray-700 active:bg-gray-800 transition-colors disabled:bg-gray-400"
          >
            Ops editor
          </button>

          <Link
            to="/multifile"
            className="px-4 py-2 bg-gray-600 text-white rounded hover:bg-gray-700 active:bg-gray-800 transition-colors"
          >
            Back to Multifile Home
          </Link>

          <Link
            to="/"
            className="px-4 py-2 bg-gray-600 text-white rounded hover:bg-gray-700 active:bg-gray-800 transition-colors"
          >
            Back
          </Link>
        </div>
      </div>

      {err && <p className="text-gray-700">{err}</p>}
      {viewportBlock}
    </div>
  );

  // FULL mode replaces the page
  if (editorKind === "sql" && editorMode === "full") {
    return (
      <MultiSqlEditor open={true} mode="full" datasets={datasets} onClose={() => setEditorKind(null)} />
    );
  }

  if (editorKind === "ops" && editorMode === "full") {
    return (
      <div className="min-h-screen bg-gray-50">
        <div className="max-w-6xl mx-auto p-6">
          <div className="flex items-center justify-between">
            <div className="text-xl font-semibold text-gray-900">Multifile — Ops editor</div>
            <button
              className="px-4 py-2 bg-gray-600 text-white rounded hover:bg-gray-700 active:bg-gray-800 transition-colors"
              type="button"
              onClick={() => setEditorKind(null)}
            >
              Back
            </button>
          </div>

          {/* ✅ allow page scroll; editor itself is scrollable too */}
          <div className="mt-4">
            <MultiOpsEditor datasets={datasets} mode="full" onClose={() => setEditorKind(null)} />
          </div>
        </div>
      </div>
    );
  }

  // SPLIT mode shows the page + side panel
  if (editorKind === "sql" && editorMode === "split") {
    return (
      <div className="min-h-screen bg-gray-50 flex flex-col lg:flex-row">
        <div className="flex-1 overflow-auto">{page}</div>
        <div className="w-full lg:w-[46%] border-t lg:border-t-0 lg:border-l border-gray-300 bg-gray-50 overflow-auto">
          <MultiSqlEditor
            open={true}
            mode="split"
            datasets={datasets}
            onClose={() => setEditorKind(null)}
          />
        </div>
      </div>
    );
  }

  if (editorKind === "ops" && editorMode === "split") {
    return (
      <div className="min-h-screen bg-gray-50 flex flex-col lg:flex-row">
        <div className="flex-1 overflow-auto">{page}</div>
        <div className="w-full lg:w-[46%] border-t lg:border-t-0 lg:border-l border-gray-300 bg-gray-50 overflow-auto">
          <MultiOpsEditor datasets={datasets} mode="split" onClose={() => setEditorKind(null)} />
        </div>
      </div>
    );
  }

  // OVERLAY mode renders the page normally + modal overlay
  return (
    <div className="min-h-screen bg-gray-50">
      {page}

      <MultiSqlEditor
        open={editorKind === "sql"}
        mode="overlay"
        datasets={datasets}
        onClose={() => setEditorKind(null)}
      />

      {editorKind === "ops" && (
        <MultiOpsEditor datasets={datasets} mode="overlay" onClose={() => setEditorKind(null)} />
      )}
    </div>
  );
}
