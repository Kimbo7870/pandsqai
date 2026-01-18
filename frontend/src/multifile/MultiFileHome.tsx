import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { Link, useNavigate } from "react-router-dom";
import {
  deleteAllMultiDatasets,
  deleteMultiDataset,
  getMultiCurrent,
  uploadMultiDataset,
} from "../lib/multifileApi";
import type { MultiDatasetListItem, PreviewDims } from "../lib/multifileTypes";
import DatasetPreviewModal, { type PreviewDataset } from "./DatasetPreviewModal";

const PREVIEW_DIMS_KEY = "pandsqai.multifile.previewDims.v1";

function clampPreviewDims(d: PreviewDims): PreviewDims {
  const rows = Math.max(1, Math.min(100, Math.trunc(d.rows)));
  const cols = Math.max(1, Math.min(100, Math.trunc(d.cols)));
  return { rows, cols };
}

function readPreviewDimsFromStorage(fallback: PreviewDims): PreviewDims {
  try {
    const raw = localStorage.getItem(PREVIEW_DIMS_KEY);
    if (!raw) return fallback;
    const parsed = JSON.parse(raw) as unknown;
    if (
      typeof parsed === "object" &&
      parsed !== null &&
      "rows" in parsed &&
      "cols" in parsed
    ) {
      const rows = Number((parsed as { rows: unknown }).rows);
      const cols = Number((parsed as { cols: unknown }).cols);
      if (Number.isFinite(rows) && Number.isFinite(cols)) return clampPreviewDims({ rows, cols });
    }
    return fallback;
  } catch {
    return fallback;
  }
}

function writePreviewDimsToStorage(d: PreviewDims) {
  try {
    localStorage.setItem(PREVIEW_DIMS_KEY, JSON.stringify(d));
  } catch {
    // ignore
  }
}

function formatUploadedAt(v: string | null): string {
  if (!v) return "—";
  const d = new Date(v);
  if (Number.isNaN(d.getTime())) return "—";
  return d.toLocaleString();
}

export default function MultiFileHome() {
  const navigate = useNavigate();

  const defaultDims = useMemo<PreviewDims>(() => ({ rows: 20, cols: 20 }), []);
  const initialDims = useMemo(() => readPreviewDimsFromStorage(defaultDims), [defaultDims]);

  const [previewRows, setPreviewRows] = useState<number>(initialDims.rows);
  const [previewCols, setPreviewCols] = useState<number>(initialDims.cols);

  const [datasets, setDatasets] = useState<MultiDatasetListItem[]>([]);
  const [loading, setLoading] = useState<boolean>(true);
  const [err, setErr] = useState<string>("");

  const [file, setFile] = useState<File | null>(null);
  const [mutating, setMutating] = useState<boolean>(false);
  const [toast, setToast] = useState<string>("");
  const toastTimer = useRef<number | null>(null);

  const [previewOpen, setPreviewOpen] = useState<boolean>(false);
  const [previewDataset, setPreviewDataset] = useState<PreviewDataset | null>(null);

  const atCapacity = datasets.length >= 3;

  function showToast(message: string) {
    setToast(message);
    if (toastTimer.current) window.clearTimeout(toastTimer.current);
    toastTimer.current = window.setTimeout(() => setToast(""), 1000);
  }

  function openPreview(d: MultiDatasetListItem) {
    setPreviewDataset({ dataset_id: d.dataset_id, display_name: d.display_name });
    setPreviewOpen(true);
  }

  function closePreview() {
    setPreviewOpen(false);
  }

  const refresh = useCallback(async () => {
    setLoading(true);
    setErr("");
    try {
      const r = await getMultiCurrent();
      setDatasets(r.datasets);
    } catch (e) {
      setErr(e instanceof Error ? e.message : "Failed to load datasets");
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    void refresh();
    return () => {
      if (toastTimer.current) window.clearTimeout(toastTimer.current);
    };
  }, [refresh]);

  useEffect(() => {
    const d = clampPreviewDims({ rows: previewRows, cols: previewCols });
    writePreviewDimsToStorage(d);
  }, [previewRows, previewCols]);

  async function onUpload() {
    if (!file) return;
    setMutating(true);
    setErr("");
    try {
      const resp = await uploadMultiDataset(file);
      if (resp.already_present) showToast("Already added");
      setFile(null);
      await refresh();
    } catch (e) {
      setErr(e instanceof Error ? e.message : "Upload failed");
    } finally {
      setMutating(false);
    }
  }

  async function onDeleteOne(dataset_id: string) {
    setMutating(true);
    setErr("");
    try {
      await deleteMultiDataset(dataset_id);
      await refresh();
    } catch (e) {
      setErr(e instanceof Error ? e.message : "Delete failed");
    } finally {
      setMutating(false);
    }
  }

  async function onDeleteAll() {
    setMutating(true);
    setErr("");
    try {
      await deleteAllMultiDatasets();
      await refresh();
    } catch (e) {
      setErr(e instanceof Error ? e.message : "Delete all failed");
    } finally {
      setMutating(false);
    }
  }

  function onChangeRows(v: string) {
    const n = Number(v);
    if (!Number.isFinite(n)) return;
    setPreviewRows(Math.max(1, Math.min(100, Math.trunc(n))));
  }

  function onChangeCols(v: string) {
    const n = Number(v);
    if (!Number.isFinite(n)) return;
    setPreviewCols(Math.max(1, Math.min(100, Math.trunc(n))));
  }

  return (
    <div className="min-h-screen bg-gray-50">
      <div className="max-w-5xl mx-auto px-4 py-6">
        <div className="flex items-center justify-between">
          <div className="min-w-0">
            <h1 className="text-2xl font-semibold text-gray-900">Multifile</h1>
            <p className="text-sm text-gray-600">
              Upload up to 3 datasets. Aliases will be t1, t2, t3 based on order.
            </p>
          </div>
          <div className="flex gap-2">
            <Link
              to="/"
              className="px-3 py-2 border border-gray-300 rounded text-gray-800 hover:bg-gray-100"
            >
              Back
            </Link>
            <button
              type="button"
              className={`px-3 py-2 border rounded ${
                datasets.length === 0
                  ? "border-gray-200 text-gray-400 cursor-not-allowed"
                  : "border-gray-300 text-gray-800 hover:bg-gray-100"
              }`}
              disabled={datasets.length === 0}
              onClick={() => navigate("/multifile/view-all")}
            >
              View all
            </button>
          </div>
        </div>

        {toast && (
          <div className="mt-4 flex justify-center">
            <div className="px-3 py-2 bg-gray-900 text-white rounded shadow text-sm">{toast}</div>
          </div>
        )}

        {err && (
          <div className="mt-4 border border-gray-200 bg-white rounded p-3 text-sm text-gray-800">
            {err}
          </div>
        )}

        <div className="mt-6 grid grid-cols-1 md:grid-cols-3 gap-4">
          <div className="md:col-span-1 bg-white border border-gray-200 rounded p-4">
            <h2 className="text-sm font-medium text-gray-900">Preview size</h2>
            <p className="text-xs text-gray-600 mt-1">Used for previews and view-all windows.</p>

            <div className="mt-3 grid grid-cols-2 gap-3">
              <div>
                <label className="block text-xs text-gray-700">Rows</label>
                <input
                  type="number"
                  min={1}
                  max={100}
                  value={previewRows}
                  onChange={(e) => onChangeRows(e.target.value)}
                  className="mt-1 w-full px-2 py-1 border border-gray-300 rounded bg-white"
                />
              </div>
              <div>
                <label className="block text-xs text-gray-700">Cols</label>
                <input
                  type="number"
                  min={1}
                  max={100}
                  value={previewCols}
                  onChange={(e) => onChangeCols(e.target.value)}
                  className="mt-1 w-full px-2 py-1 border border-gray-300 rounded bg-white"
                />
              </div>
            </div>
          </div>

          <div className="md:col-span-2 bg-white border border-gray-200 rounded p-4">
            <h2 className="text-sm font-medium text-gray-900">Upload dataset</h2>
            <p className="text-xs text-gray-600 mt-1">Accepts .csv or .parquet. Max 3 loaded.</p>

            <div className="mt-3 flex flex-col md:flex-row gap-3 md:items-center">
              <input
                type="file"
                accept=".csv,.parquet"
                disabled={atCapacity || mutating}
                onChange={(e) => setFile(e.target.files?.[0] ?? null)}
                className="block w-full text-sm text-gray-700"
              />
              <button
                type="button"
                onClick={() => void onUpload()}
                disabled={!file || atCapacity || mutating}
                className={`px-3 py-2 rounded border ${
                  !file || atCapacity || mutating
                    ? "border-gray-200 text-gray-400 cursor-not-allowed"
                    : "border-gray-300 text-gray-800 hover:bg-gray-100"
                }`}
              >
                Upload
              </button>
            </div>

            {atCapacity && (
              <div className="mt-3 text-xs text-gray-600">
                Capacity reached (3 datasets). Delete one to upload another.
              </div>
            )}
          </div>
        </div>

        <div className="mt-6 bg-white border border-gray-200 rounded">
          <div className="flex items-center justify-between px-4 py-3 border-b border-gray-200">
            <h2 className="text-sm font-medium text-gray-900">Current datasets</h2>
            <button
              type="button"
              onClick={() => void onDeleteAll()}
              disabled={datasets.length === 0 || mutating}
              className={`px-3 py-2 rounded border ${
                datasets.length === 0 || mutating
                  ? "border-gray-200 text-gray-400 cursor-not-allowed"
                  : "border-gray-300 text-gray-800 hover:bg-gray-100"
              }`}
            >
              Delete all
            </button>
          </div>

          {loading ? (
            <div className="p-4 text-sm text-gray-600">Loading…</div>
          ) : datasets.length === 0 ? (
            <div className="p-4 text-sm text-gray-600">
              No datasets loaded. Upload up to 3 to begin.
            </div>
          ) : (
            <div className="overflow-x-auto">
              <table className="min-w-full text-sm">
                <thead className="bg-gray-50 border-b border-gray-200">
                  <tr>
                    <th className="text-left font-medium text-gray-700 px-4 py-2">Name</th>
                    <th className="text-left font-medium text-gray-700 px-4 py-2">Rows</th>
                    <th className="text-left font-medium text-gray-700 px-4 py-2">Cols</th>
                    <th className="text-left font-medium text-gray-700 px-4 py-2">Uploaded</th>
                    <th className="text-right font-medium text-gray-700 px-4 py-2">Actions</th>
                  </tr>
                </thead>
                <tbody>
                  {datasets.map((d) => (
                    <tr
                      key={d.dataset_id}
                      className="odd:bg-gray-50 hover:bg-gray-100 cursor-pointer"
                      onClick={() => openPreview(d)}
                      title="Click to preview"
                    >
                      <td className="px-4 py-2 text-gray-900 whitespace-nowrap">
                        {d.display_name}
                      </td>
                      <td className="px-4 py-2 text-gray-700">{d.n_rows}</td>
                      <td className="px-4 py-2 text-gray-700">{d.n_cols}</td>
                      <td className="px-4 py-2 text-gray-700">{formatUploadedAt(d.uploaded_at)}</td>
                      <td className="px-4 py-2 text-right">
                        <div className="inline-flex gap-2">
                          <button
                            type="button"
                            onClick={(e) => {
                              e.stopPropagation();
                              openPreview(d);
                            }}
                            className="px-3 py-1 rounded border border-gray-300 text-gray-800 hover:bg-gray-100"
                          >
                            Preview
                          </button>
                          <button
                            type="button"
                            onClick={(e) => {
                              e.stopPropagation();
                              void onDeleteOne(d.dataset_id);
                            }}
                            disabled={mutating}
                            className={`px-3 py-1 rounded border ${
                              mutating
                                ? "border-gray-200 text-gray-400 cursor-not-allowed"
                                : "border-gray-300 text-gray-800 hover:bg-gray-100"
                            }`}
                          >
                            Delete
                          </button>
                        </div>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>

              <div className="px-4 py-3 text-xs text-gray-600 border-t border-gray-200">
                Tip: Click a row (or Preview) to open the preview modal.
              </div>
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
    </div>
  );
}
