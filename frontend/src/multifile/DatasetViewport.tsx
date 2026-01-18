import { type PointerEvent, useEffect, useMemo, useRef, useState } from "react";
import { getMultiChunk } from "../lib/multifileApi";
import type { CellLike, MultiChunkResponse } from "../lib/multifileTypes";

type Props = {
  dataset_id: string;
  title: string;
  viewRows: number;
  viewCols: number;
};

const ROW_HEIGHT_PX = 24;
const COL_WIDTH_PX = 140;
const THROTTLE_MS = 90;

function cellToText(v: CellLike): string {
  if (v === null) return "";
  return String(v);
}

function clampInt(v: number, lo: number, hi: number): number {
  if (!Number.isFinite(v)) return lo;
  return Math.max(lo, Math.min(hi, Math.trunc(v)));
}

export default function DatasetViewport({ dataset_id, title, viewRows, viewCols }: Props) {
  const dims = useMemo(() => {
    return { rows: clampInt(viewRows, 1, 100), cols: clampInt(viewCols, 1, 100) };
  }, [viewRows, viewCols]);

  const [rowStart, setRowStart] = useState<number>(0);
  const [colStart, setColStart] = useState<number>(0);
  const [totalRows, setTotalRows] = useState<number>(0);
  const [totalCols, setTotalCols] = useState<number>(0);
  const [loading, setLoading] = useState<boolean>(true);
  const [err, setErr] = useState<string>("");
  const [chunk, setChunk] = useState<MultiChunkResponse | null>(null);
  const [dragging, setDragging] = useState<boolean>(false);

  const abortRef = useRef<AbortController | null>(null);
  const reqIdRef = useRef<number>(0);

  const draggingRef = useRef<boolean>(false);
  const startRef = useRef<{ x: number; y: number; row: number; col: number } | null>(null);

  const timerRef = useRef<number | null>(null);
  const pendingRef = useRef<{ row: number; col: number } | null>(null);

  function maxRowStart(): number {
    return Math.max(0, totalRows - dims.rows);
  }
  function maxColStart(): number {
    return Math.max(0, totalCols - dims.cols);
  }

  function schedulePanUpdate(nextRow: number, nextCol: number) {
    const r = clampInt(nextRow, 0, maxRowStart());
    const c = clampInt(nextCol, 0, maxColStart());
    pendingRef.current = { row: r, col: c };

    if (timerRef.current !== null) return;

    timerRef.current = window.setTimeout(() => {
      timerRef.current = null;
      const p = pendingRef.current;
      pendingRef.current = null;
      if (!p) return;

      setRowStart((prev) => (prev === p.row ? prev : p.row));
      setColStart((prev) => (prev === p.col ? prev : p.col));
    }, THROTTLE_MS);
  }

  function flushPanUpdate() {
    if (timerRef.current !== null) {
      window.clearTimeout(timerRef.current);
      timerRef.current = null;
    }
    const p = pendingRef.current;
    pendingRef.current = null;
    if (!p) return;
    setRowStart((prev) => (prev === p.row ? prev : p.row));
    setColStart((prev) => (prev === p.col ? prev : p.col));
  }

  useEffect(() => {
    abortRef.current?.abort();
    const ac = new AbortController();
    abortRef.current = ac;

    const myReqId = ++reqIdRef.current;

    setLoading(true);
    setErr("");

    (async () => {
      try {
        const j = await getMultiChunk(
          dataset_id,
          rowStart,
          colStart,
          dims.rows,
          dims.cols,
          ac.signal
        );
        if (reqIdRef.current !== myReqId) return;

        setChunk(j);
        setTotalRows(j.total_rows);
        setTotalCols(j.total_cols);

        // If backend clamped our starts, sync them.
        if (j.row_start !== rowStart) setRowStart(j.row_start);
        if (j.col_start !== colStart) setColStart(j.col_start);
      } catch (e) {
        if (e instanceof DOMException && e.name === "AbortError") return;
        if (reqIdRef.current !== myReqId) return;
        setErr(e instanceof Error ? e.message : "Failed to load chunk");
      } finally {
        const isLatest = reqIdRef.current === myReqId;
        if (isLatest) setLoading(false);
      }
    })();

    return () => {
      ac.abort();
    };
  }, [dataset_id, rowStart, colStart, dims.rows, dims.cols]);

  useEffect(() => {
    return () => {
      abortRef.current?.abort();
      if (timerRef.current !== null) window.clearTimeout(timerRef.current);
    };
  }, []);

  function onPointerDown(e: PointerEvent<HTMLDivElement>) {
    setDragging(true);
    draggingRef.current = true;
    startRef.current = { x: e.clientX, y: e.clientY, row: rowStart, col: colStart };
    (e.currentTarget as HTMLDivElement).setPointerCapture(e.pointerId);
  }

  function onPointerMove(e: PointerEvent<HTMLDivElement>) {
    if (!draggingRef.current || !startRef.current) return;
    const dx = e.clientX - startRef.current.x;
    const dy = e.clientY - startRef.current.y;
    const dCols = Math.round(dx / COL_WIDTH_PX);
    const dRows = Math.round(dy / ROW_HEIGHT_PX);
    const nextRow = startRef.current.row - dRows;
    const nextCol = startRef.current.col - dCols;
    schedulePanUpdate(nextRow, nextCol);
  }

  function stopDrag() {
    setDragging(false);
    draggingRef.current = false;
    startRef.current = null;
    flushPanUpdate();
  }

  const cursor = dragging ? "grabbing" : "grab";
  const rangeText = chunk
    ? `rows ${chunk.row_start + 1}-${chunk.row_start + chunk.n_rows} of ${chunk.total_rows}, cols ${
        chunk.col_start + 1
      }-${chunk.col_start + chunk.n_cols} of ${chunk.total_cols}`
    : "";

  return (
    <div className="bg-white border border-gray-300 rounded shadow-sm overflow-hidden w-[520px]">
      <div className="px-3 py-2 border-b border-gray-200 flex items-center justify-between">
        <div className="min-w-0">
          <p className="text-sm font-medium text-gray-900 truncate">{title}</p>
          {rangeText && <p className="text-xs text-gray-600 truncate">{rangeText}</p>}
        </div>
        <div className="text-xs text-gray-600 whitespace-nowrap">
          {dims.rows}×{dims.cols}
        </div>
      </div>

      <div
        className="h-[340px] overflow-hidden select-none"
        style={{ cursor, touchAction: "none" }}
        onPointerDown={onPointerDown}
        onPointerMove={onPointerMove}
        onPointerUp={stopDrag}
        onPointerCancel={stopDrag}
        onPointerLeave={() => {
          if (draggingRef.current) stopDrag();
        }}
      >
        {loading ? (
          <div className="p-3 text-sm text-gray-600">Loading…</div>
        ) : err ? (
          <div className="p-3 text-sm text-gray-700">{err}</div>
        ) : !chunk ? (
          <div className="p-3 text-sm text-gray-600">No data.</div>
        ) : (
          <div className="h-full">
            <table className="w-full text-sm table-fixed">
              <thead>
                <tr className="bg-gray-50 border-b border-gray-200">
                  {chunk.columns.map((c) => (
                    <th
                      key={c}
                      className="text-left text-xs font-medium text-gray-700 px-2 py-2"
                      style={{ width: COL_WIDTH_PX }}
                      title={c}
                    >
                      <span className="block whitespace-nowrap overflow-hidden text-ellipsis">
                        {c}
                      </span>
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {chunk.rows.map((row, i) => (
                  <tr key={i} className="border-b border-gray-100">
                    {row.map((cell, j) => (
                      <td
                        key={j}
                        className="px-2"
                        style={{ width: COL_WIDTH_PX, height: ROW_HEIGHT_PX }}
                        title={cellToText(cell)}
                      >
                        <span className="block whitespace-nowrap overflow-hidden text-ellipsis">
                          {cellToText(cell)}
                        </span>
                      </td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>

            <div className="px-3 py-2 text-xs text-gray-600 border-t border-gray-200 bg-white">
              Drag to pan (2D). Panning is clamped to dataset bounds.
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
