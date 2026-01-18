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

function clampInt(n: number, lo: number, hi: number): number {
  if (Number.isNaN(n)) return lo;
  return Math.min(hi, Math.max(lo, Math.trunc(n)));
}

function cellToText(v: CellLike): string {
  if (v === null) return "";
  return String(v);
}

export default function DatasetViewport({ dataset_id, title, viewRows, viewCols }: Props) {
  const dims = useMemo(() => {
    return {
      rows: clampInt(viewRows, 1, 100),
      cols: clampInt(viewCols, 1, 100),
    };
  }, [viewRows, viewCols]);

  const [rowStart, setRowStart] = useState<number>(0);
  const [colStart, setColStart] = useState<number>(0);
  const [loading, setLoading] = useState<boolean>(true);
  const [err, setErr] = useState<string>("");
  const [chunk, setChunk] = useState<MultiChunkResponse | null>(null);
  const [dragging, setDragging] = useState<boolean>(false);

  const abortRef = useRef<AbortController | null>(null);
  const reqIdRef = useRef<number>(0);

  const draggingRef = useRef<boolean>(false);
  const startRef = useRef<{ x: number; y: number; row: number; col: number } | null>(null);

  const pendingRef = useRef<{ row: number; col: number } | null>(null);
  const timerRef = useRef<number | null>(null);

  const totalRows = chunk?.total_rows ?? 0;
  const totalCols = chunk?.total_cols ?? 0;
  const maxRowStart = Math.max(0, totalRows - dims.rows);
  const maxColStart = Math.max(0, totalCols - dims.cols);

  function schedulePanUpdate(nextRow: number, nextCol: number) {
    const row = clampInt(nextRow, 0, maxRowStart);
    const col = clampInt(nextCol, 0, maxColStart);

    pendingRef.current = { row, col };
    if (timerRef.current !== null) return;

    timerRef.current = window.setTimeout(() => {
      timerRef.current = null;
      const pending = pendingRef.current;
      pendingRef.current = null;
      if (!pending) return;

      setRowStart((prev) => (prev === pending.row ? prev : pending.row));
      setColStart((prev) => (prev === pending.col ? prev : pending.col));
    }, THROTTLE_MS);
  }

  function flushPanUpdate() {
    if (timerRef.current !== null) {
      window.clearTimeout(timerRef.current);
      timerRef.current = null;
    }
    const pending = pendingRef.current;
    pendingRef.current = null;
    if (!pending) return;

    setRowStart((prev) => (prev === pending.row ? prev : pending.row));
    setColStart((prev) => (prev === pending.col ? prev : pending.col));
  }

  useEffect(() => {
    if (!chunk) return;
    if (rowStart > maxRowStart) setRowStart(maxRowStart);
    if (colStart > maxColStart) setColStart(maxColStart);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [dims.rows, dims.cols, totalRows, totalCols]);

  useEffect(() => {
    abortRef.current?.abort();
    const ac = new AbortController();
    abortRef.current = ac;

    const myReqId = ++reqIdRef.current;

    setLoading(true);
    setErr("");

    (async () => {
      try {
        const res = await getMultiChunk(
          dataset_id,
          rowStart,
          colStart,
          dims.rows,
          dims.cols,
          ac.signal
        );

        if (reqIdRef.current !== myReqId) return;

        setChunk(res);

        // reconcile server clamps
        if (res.row_start !== rowStart) setRowStart(res.row_start);
        if (res.col_start !== colStart) setColStart(res.col_start);
      } catch (e) {
        if (e instanceof DOMException && e.name === "AbortError") return;
        if (reqIdRef.current !== myReqId) return;
        setErr(e instanceof Error ? e.message : "Failed to load chunk");
      } finally {
        // ✅ no return in finally (fixes no-unsafe-finally)
        const stale = reqIdRef.current !== myReqId;
        if (!stale) setLoading(false);
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

  function onPointerDownCapture(e: PointerEvent<HTMLDivElement>) {
    if (e.button !== 0) return;
    e.preventDefault();

    setDragging(true);
    draggingRef.current = true;
    startRef.current = { x: e.clientX, y: e.clientY, row: rowStart, col: colStart };

    try {
      (e.currentTarget as HTMLDivElement).setPointerCapture(e.pointerId);
    } catch {
      // ignore
    }
  }

  function onPointerMove(e: PointerEvent<HTMLDivElement>) {
    if (!draggingRef.current || !startRef.current) return;
    e.preventDefault();

    const dx = e.clientX - startRef.current.x;
    const dy = e.clientY - startRef.current.y;

    const dCols = Math.round(dx / COL_WIDTH_PX);
    const dRows = Math.round(dy / ROW_HEIGHT_PX);

    schedulePanUpdate(startRef.current.row - dRows, startRef.current.col - dCols);
  }

  function stopDrag(e?: PointerEvent<HTMLDivElement>) {
    if (e) {
      try {
        (e.currentTarget as HTMLDivElement).releasePointerCapture(e.pointerId);
      } catch {
        // ignore
      }
    }
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
        style={{ cursor, touchAction: "none", userSelect: "none" }}
        onPointerDownCapture={onPointerDownCapture}
        onPointerMove={onPointerMove}
        onPointerUp={stopDrag}
        onPointerCancel={stopDrag}
      >
        {loading ? (
          <div className="p-3 text-sm text-gray-600">Loading…</div>
        ) : err ? (
          <div className="p-3 text-sm text-gray-700">{err}</div>
        ) : !chunk ? (
          <div className="p-3 text-sm text-gray-600">No data.</div>
        ) : (
          <div className="w-full h-full overflow-auto">
            <table className="text-sm border-separate border-spacing-0 min-w-max">
              <thead>
                <tr>
                  {chunk.columns.map((c) => (
                    <th
                      key={c}
                      className="sticky top-0 bg-gray-50 px-2 py-2 text-left border-b border-gray-200 font-medium text-gray-700"
                      style={{ width: COL_WIDTH_PX, maxWidth: COL_WIDTH_PX }}
                    >
                      <div className="whitespace-nowrap overflow-hidden text-ellipsis" title={c}>
                        {c}
                      </div>
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {chunk.rows.map((row, i) => (
                  <tr key={i} className={i % 2 === 0 ? "bg-white" : "bg-gray-50"}>
                    {row.map((cell, j) => {
                      const text = cellToText(cell);
                      return (
                        <td
                          key={j}
                          className="px-2 border-b border-gray-200 align-middle"
                          style={{
                            height: ROW_HEIGHT_PX,
                            width: COL_WIDTH_PX,
                            maxWidth: COL_WIDTH_PX,
                          }}
                          title={text}
                        >
                          <div className="whitespace-nowrap overflow-hidden text-ellipsis">{text}</div>
                        </td>
                      );
                    })}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>

      <div className="px-3 py-2 border-t border-gray-200 text-xs text-gray-600">
        Drag to pan (hold and move).
      </div>
    </div>
  );
}
