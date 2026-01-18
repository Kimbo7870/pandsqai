import { useEffect, useMemo, useRef, useState } from "react";
import { Link, useNavigate } from "react-router-dom";
import {
  deleteAllMultiDatasets,
  deleteMultiDataset,
  getMultiCurrent,
  uploadMultiDataset,
} from "../lib/multifileApi";
import type { MultiDatasetListItem, PreviewDims } from "../lib/multifileTypes";
import DatasetPreviewModal, { type PreviewDataset } from "./DatasetPreviewModal";
import MultiSqlEditor, { type MultiEditorMode } from "./MultiSqlEditor";
import { MultiOpsEditor } from "./MultiOpsEditor";

const PREVIEW_DIMS_KEY = "pandsqai.multifile.previewDims.v1";

type EditorKind = "sql" | "ops" | null;

function clampInt(n: number, lo: number, hi: number): number {
  if (Number.isNaN(n)) return lo;
  return Math.min(hi, Math.max(lo, Math.trunc(n)));
}

function readPreviewDimsFromStorage(defaultDims: PreviewDims): PreviewDims {
  try {
    const raw = localStorage.getItem(PREVIEW_DIMS_KEY);
    if (!raw) return defaultDims;
    const parsed: unknown = JSON.parse(raw);
    if (!parsed || typeof parsed !== "object") return defaultDims;
    const obj = parsed as { rows?: unknown; cols?: unknown };
    const rows = clampInt(Number(obj.rows), 1, 100);
    const cols = clampInt(Number(obj.cols), 1, 100);
    return { rows, cols };
  } catch {
    return defaultDims;
  }
}

function writePreviewDimsToStorage(dims: PreviewDims) {
  try {
    localStorage.setItem(PREVIEW_DIMS_KEY, JSON.stringify(dims));
  } catch {
    // ignore
  }
}

function formatUploadedAt(uploaded_at: string | null): string {
  if (!uploaded_at) return "—";
  const dt = new Date(uploaded_at);
  if (Number.isNaN(dt.getTime())) return "—";
  return dt.toLocaleString();
}

function parseApiErrorMessage(msg: string): { code?: string; detail?: string } {
  try {
    const parsed = JSON.parse(msg) as unknown;
    if (!parsed || typeof parsed !== "object") return {};
    const root = parsed as { detail?: unknown };
    if (!root.detail || typeof root.detail !== "object") return {};
    const d = root.detail as { code?: unknown; detail?: unknown };
    const code = typeof d.code === "string" ? d.code : undefined;
    const detail = typeof d.detail === "string" ? d.detail : undefined;
    return { code, detail };
  } catch {
    return {};
  }
}

function toUserError(e: unknown): string {
  if (e instanceof Error) {
    const { code, detail } = parseApiErrorMessage(e.message);
    if (code && detail) return `${code}: ${detail}`;
    return e.message;
  }
  return "Something went wrong";
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

export default function MultiFileHome() {
  const navigate = useNavigate();

  const defaultDims = useMemo<PreviewDims>(() => ({ rows: 20, cols: 20 }), []);
  const initialDims = useMemo(() => readPreviewDimsFromStorage(defaultDims), [defaultDims]);

  const [datasets, setDatasets] = useState<MultiDatasetListItem[]>([]);
  const [loading, setLoading] = useState<boolean>(true);
  const [err, setErr] = useState<string>("");

  const [previewRows, setPreviewRows] = useState<number>(initialDims.rows);
  const [previewCols, setPreviewCols] = useState<number>(initialDims.cols);

  const [selectedFile, setSelectedFile] = useState<File | null>(null);
  const [uploading, setUploading] = useState<boolean>(false);
  const [mutating, setMutating] = useState<boolean>(false);

  const [toast, setToast] = useState<string>("");
  const toastTimer = useRef<number | null>(null);

  const [previewOpen, setPreviewOpen] = useState<boolean>(false);
  const [previewDataset, setPreviewDataset] = useState<PreviewDataset | null>(null);

  const [editorKind, setEditorKind] = useState<EditorKind>(null);
  const [editorMode, setEditorMode] = useState<MultiEditorMode>("overlay");

  function showToast(message: string) {
    setToast(message);
    const timerId = toastTimer.current;
    if (timerId) window.clearTimeout(timerId);
    toastTimer.current = window.setTimeout(() => setToast(""), 1000);
  }

  function openPreview(d: MultiDatasetListItem) {
    setPreviewDataset({ dataset_id: d.dataset_id, display_name: d.display_name });
    setPreviewOpen(true);
  }

  function closePreview() {
    setPreviewOpen(false);
    setPreviewDataset(null);
  }

  async function refresh() {
    setErr("");
    const res = await getMultiCurrent();
    setDatasets(res.datasets);
  }

  useEffect(() => {
    let cancelled = false;
    (async () => {
      setLoading(true);
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
      const timerId = toastTimer.current;
      if (timerId) window.clearTimeout(timerId);
    };
  }, []);

  useEffect(() => {
    writePreviewDimsToStorage({ rows: previewRows, cols: previewCols });
  }, [previewRows, previewCols]);

  const slotsFull = datasets.length >= 3;

  async function onUpload() {
    if (!selectedFile) return;
    setErr("");
    setUploading(true);
    try {
      const res = await uploadMultiDataset(selectedFile);
      await refresh();
      if (res.already_present) showToast("Already added");
      setSelectedFile(null);
    } catch (e) {
      setErr(toUserError(e));
    } finally {
      setUploading(false);
    }
  }

  async function onDeleteOne(dataset_id: string) {
    setErr("");
    setMutating(true);
    try {
      await deleteMultiDataset(dataset_id);
      await refresh();
    } catch (e) {
      setErr(toUserError(e));
    } finally {
      setMutating(false);
    }
  }

  async function onDeleteAll() {
    setErr("");
    setMutating(true);
    try {
      await deleteAllMultiDatasets();
      await refresh();
    } catch (e) {
      setErr(toUserError(e));
    } finally {
      setMutating(false);
    }
  }

  const page = (
    <>
      <div className="max-w-5xl mx-auto p-6 space-y-6">
        <div className="flex items-center justify-between gap-3">
          <h1 className="text-2xl font-semibold text-gray-900">Multifile</h1>

          <div className="flex flex-wrap gap-2 justify-end items-center">
            <ModeSelector mode={editorMode} onChange={setEditorMode} disabled={editorKind !== null} />

            <button
              onClick={() => setEditorKind("sql")}
              disabled={datasets.length === 0}
              className="px-4 py-2 bg-gray-600 text-white rounded hover:bg-gray-700 active:bg-gray-800 transition-colors disabled:bg-gray-400"
              type="button"
            >
              SQL editor
            </button>

            <button
              onClick={() => setEditorKind("ops")}
              disabled={datasets.length === 0}
              className="px-4 py-2 bg-gray-600 text-white rounded hover:bg-gray-700 active:bg-gray-800 transition-colors disabled:bg-gray-400"
              type="button"
            >
              Ops editor
            </button>

            <button
              onClick={() => navigate("/multifile/view-all")}
              disabled={datasets.length === 0}
              className="px-4 py-2 bg-gray-600 text-white rounded hover:bg-gray-700 active:bg-gray-800 transition-colors disabled:bg-gray-400"
              type="button"
            >
              View all
            </button>

            <Link
              to="/"
              className="px-4 py-2 bg-gray-600 text-white rounded hover:bg-gray-700 active:bg-gray-800 transition-colors"
            >
              Back
            </Link>
          </div>
        </div>

        {toast && (
          <div className="flex justify-center">
            <div className="px-3 py-2 bg-gray-900 text-white text-sm rounded shadow">{toast}</div>
          </div>
        )}

        <div className="border border-gray-300 rounded p-4 bg-white space-y-3">
          <div>
            <p className="font-medium text-gray-900">Preview dimensions</p>
            <p className="text-sm text-gray-600">Values are clamped to 1–100 and saved locally.</p>
          </div>

          <div className="flex flex-wrap gap-4">
            <label className="flex items-center gap-2">
              <span className="text-sm text-gray-700">Rows</span>
              <input
                type="number"
                inputMode="numeric"
                min={1}
                max={100}
                value={previewRows}
                onChange={(e) => setPreviewRows(clampInt(Number(e.target.value), 1, 100))}
                className="w-24 px-2 py-1 border border-gray-300 rounded"
              />
            </label>
            <label className="flex items-center gap-2">
              <span className="text-sm text-gray-700">Cols</span>
              <input
                type="number"
                inputMode="numeric"
                min={1}
                max={100}
                value={previewCols}
                onChange={(e) => setPreviewCols(clampInt(Number(e.target.value), 1, 100))}
                className="w-24 px-2 py-1 border border-gray-300 rounded"
              />
            </label>
          </div>
        </div>

        <div className="border border-gray-300 rounded p-4 bg-white space-y-3">
          <div className="flex items-start justify-between gap-3">
            <div>
              <p className="font-medium text-gray-900">Upload dataset (0–3)</p>
              <p className="text-sm text-gray-600">
                Upload up to 3 datasets. Duplicate content won’t consume a slot.
              </p>
            </div>
            <div className="text-sm text-gray-600">Slots: {datasets.length}/3</div>
          </div>

          <div className="flex flex-wrap items-center gap-3">
            <input
              type="file"
              accept=".csv,.parquet"
              disabled={slotsFull || uploading || mutating}
              onChange={(e) => setSelectedFile(e.target.files?.[0] ?? null)}
              className="block text-sm text-gray-700"
            />

            <button
              onClick={onUpload}
              disabled={!selectedFile || slotsFull || uploading || mutating}
              className="px-4 py-2 bg-gray-600 text-white rounded hover:bg-gray-700 active:bg-gray-800 transition-colors disabled:bg-gray-400"
              type="button"
            >
              {uploading ? "Uploading..." : "Upload"}
            </button>
          </div>

          {slotsFull && <p className="text-sm text-gray-600">Upload disabled: all 3 slots are full.</p>}
        </div>

        {err && <p className="text-gray-700">{err}</p>}

        <div className="border border-gray-300 rounded p-4 bg-white space-y-3">
          <div className="flex items-center justify-between gap-3">
            <p className="font-medium text-gray-900">Current datasets</p>
            <button
              onClick={onDeleteAll}
              disabled={datasets.length === 0 || mutating || uploading}
              className="px-4 py-2 bg-gray-600 text-white rounded hover:bg-gray-700 active:bg-gray-800 transition-colors disabled:bg-gray-400"
              type="button"
            >
              {mutating ? "Working..." : "Delete all"}
            </button>
          </div>

          {loading ? (
            <p className="text-sm text-gray-600">Loading...</p>
          ) : datasets.length === 0 ? (
            <p className="text-sm text-gray-600">No datasets loaded yet.</p>
          ) : (
            <div className="overflow-x-auto">
              <table className="min-w-full text-sm">
                <thead>
                  <tr>
                    <th className="px-2 py-2 text-left border-b border-gray-300">Name</th>
                    <th className="px-2 py-2 text-left border-b border-gray-300">Rows</th>
                    <th className="px-2 py-2 text-left border-b border-gray-300">Cols</th>
                    <th className="px-2 py-2 text-left border-b border-gray-300">Uploaded</th>
                    <th className="px-2 py-2 text-right border-b border-gray-300">Actions</th>
                  </tr>
                </thead>
                <tbody>
                  {datasets.map((d) => (
                    <tr
                      key={d.dataset_id}
                      className="odd:bg-gray-50 hover:bg-gray-100 cursor-pointer"
                      onClick={() => openPreview(d)}
                    >
                      <td className="px-2 py-2 border-b border-gray-300">{d.display_name}</td>
                      <td className="px-2 py-2 border-b border-gray-300">{d.n_rows}</td>
                      <td className="px-2 py-2 border-b border-gray-300">{d.n_cols}</td>
                      <td className="px-2 py-2 border-b border-gray-300">
                        {formatUploadedAt(d.uploaded_at)}
                      </td>
                      <td className="px-2 py-2 border-b border-gray-300 text-right">
                        <div className="inline-flex gap-2">
                          <button
                            onClick={(e) => {
                              e.stopPropagation();
                              openPreview(d);
                            }}
                            disabled={mutating || uploading}
                            className="px-3 py-1 bg-gray-600 text-white rounded hover:bg-gray-700 active:bg-gray-800 transition-colors disabled:bg-gray-400"
                            type="button"
                          >
                            Preview
                          </button>
                          <button
                            onClick={(e) => {
                              e.stopPropagation();
                              void onDeleteOne(d.dataset_id);
                            }}
                            disabled={mutating || uploading}
                            className="px-3 py-1 bg-gray-600 text-white rounded hover:bg-gray-700 active:bg-gray-800 transition-colors disabled:bg-gray-400"
                            type="button"
                          >
                            Delete
                          </button>
                        </div>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </div>
      </div>

      <DatasetPreviewModal
        open={previewOpen}
        onClose={closePreview}
        dataset={previewDataset}
        previewRows={previewRows}
        previewCols={previewCols}
      />
    </>
  );

  if (editorKind === "sql" && editorMode === "full") {
    return <MultiSqlEditor open={true} mode="full" datasets={datasets} onClose={() => setEditorKind(null)} />;
  }

  if (editorKind === "ops" && editorMode === "full") {
    return (
      <div className="min-h-screen bg-gray-50">
        <div className="max-w-6xl mx-auto p-6">
          <div className="h-[calc(100vh-48px)]">
            <MultiOpsEditor datasets={datasets} mode="full" onClose={() => setEditorKind(null)} />
          </div>
        </div>
      </div>
    );
  }

  if (editorKind === "sql" && editorMode === "split") {
    return (
      <div className="min-h-screen bg-gray-50 flex flex-col lg:flex-row">
        <div className="flex-1 overflow-auto">{page}</div>
        <div className="w-full lg:w-[46%] border-t lg:border-t-0 lg:border-l border-gray-300 bg-gray-50 overflow-auto">
          <MultiSqlEditor open={true} mode="split" datasets={datasets} onClose={() => setEditorKind(null)} />
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

  return (
    <div className="min-h-screen bg-gray-50">
      {page}

      <MultiSqlEditor open={editorKind === "sql"} mode="overlay" datasets={datasets} onClose={() => setEditorKind(null)} />

      {editorKind === "ops" && (
        <MultiOpsEditor datasets={datasets} mode="overlay" onClose={() => setEditorKind(null)} />
      )}
    </div>
  );
}
