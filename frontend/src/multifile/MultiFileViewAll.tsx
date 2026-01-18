import { useEffect, useMemo, useRef, useState } from "react";
import { Link } from "react-router-dom";
import { getMultiCurrent } from "../lib/multifileApi";
import type { MultiDatasetListItem, PreviewDims } from "../lib/multifileTypes";
import DatasetViewport from "./DatasetViewport";

const PREVIEW_DIMS_KEY = "pandsqai.multifile.previewDims.v1";

function clampPreviewDims(d: PreviewDims): PreviewDims {
  const rows = Math.max(1, Math.min(100, Math.trunc(d.rows)));
  const cols = Math.max(1, Math.min(100, Math.trunc(d.cols)));
  return { rows, cols };
}

function readPreviewDims(): PreviewDims {
  const fallback: PreviewDims = { rows: 20, cols: 20 };
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

export default function MultiFileViewAll() {
  const dims = useMemo(() => readPreviewDims(), []);

  const [datasets, setDatasets] = useState<MultiDatasetListItem[]>([]);
  const [loading, setLoading] = useState<boolean>(true);
  const [err, setErr] = useState<string>("");

  const [toast, setToast] = useState<string>("");
  const toastTimer = useRef<number | null>(null);

  function showToast(message: string) {
    setToast(message);
    if (toastTimer.current) window.clearTimeout(toastTimer.current);
    toastTimer.current = window.setTimeout(() => setToast(""), 1000);
  }

  useEffect(() => {
    (async () => {
      setLoading(true);
      setErr("");
      try {
        const r = await getMultiCurrent();
        setDatasets(r.datasets);
      } catch (e) {
        setErr(e instanceof Error ? e.message : "Failed to load multifile datasets");
      } finally {
        setLoading(false);
      }
    })();

    return () => {
      if (toastTimer.current) window.clearTimeout(toastTimer.current);
    };
  }, []);

  const layoutKind = datasets.length >= 3 ? 3 : datasets.length;

  return (
    <div className="min-h-screen bg-gray-50">
      <div className="max-w-6xl mx-auto px-4 py-6">
        <div className="flex items-center justify-between">
          <div className="min-w-0">
            <h1 className="text-2xl font-semibold text-gray-900">Multifile — View all</h1>
            <p className="text-sm text-gray-600">
              Drag inside a window to pan. Using {dims.rows}×{dims.cols} per window.
            </p>
          </div>

          <div className="flex gap-2">
            <Link
              to="/multifile"
              className="px-3 py-2 border border-gray-300 rounded text-gray-800 hover:bg-gray-100"
            >
              Back
            </Link>

            <button
              type="button"
              onClick={() => showToast("SQL editor coming in Chunk 7")}
              className="px-3 py-2 border border-gray-300 rounded text-gray-800 hover:bg-gray-100"
            >
              Open SQL
            </button>
            <button
              type="button"
              onClick={() => showToast("Ops editor coming in Chunk 8")}
              className="px-3 py-2 border border-gray-300 rounded text-gray-800 hover:bg-gray-100"
            >
              Open Ops
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

        <div className="mt-6">
          {loading ? (
            <div className="text-sm text-gray-600">Loading…</div>
          ) : datasets.length === 0 ? (
            <div className="text-sm text-gray-600">
              No datasets loaded. Go back to Multifile home to upload.
            </div>
          ) : layoutKind === 1 ? (
            <div className="flex justify-center">
              <DatasetViewport
                dataset_id={datasets[0].dataset_id}
                title={`t1 = ${datasets[0].display_name}`}
                viewRows={dims.rows}
                viewCols={dims.cols}
              />
            </div>
          ) : layoutKind === 2 ? (
            <div className="grid grid-cols-1 lg:grid-cols-2 gap-4 justify-items-center">
              {datasets.slice(0, 2).map((d, i) => (
                <DatasetViewport
                  key={d.dataset_id}
                  dataset_id={d.dataset_id}
                  title={`t${i + 1} = ${d.display_name}`}
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
                  title={`t1 = ${datasets[0].display_name}`}
                  viewRows={dims.rows}
                  viewCols={dims.cols}
                />
              </div>

              <DatasetViewport
                dataset_id={datasets[1].dataset_id}
                title={`t2 = ${datasets[1].display_name}`}
                viewRows={dims.rows}
                viewCols={dims.cols}
              />
              <DatasetViewport
                dataset_id={datasets[2].dataset_id}
                title={`t3 = ${datasets[2].display_name}`}
                viewRows={dims.rows}
                viewCols={dims.cols}
              />
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
