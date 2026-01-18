export interface MultiDatasetListItem {
  dataset_id: string;
  display_name: string;
  n_rows: number;
  n_cols: number;
  uploaded_at: string | null;
}

export interface MultiCurrentResponse {
  datasets: MultiDatasetListItem[];
}

export interface MultiUploadResponse {
  dataset_id: string;
  display_name: string;
  n_rows: number;
  n_cols: number;
  uploaded_at: string | null;
  already_present: boolean;
  slot_count: number;
}

export interface MultiDeleteResponse {
  ok: boolean;
  slot_count: number;
}

export type CellLike = string | number | boolean | null;

export interface MultiChunkResponse {
  dataset_id: string;
  total_rows: number;
  total_cols: number;
  row_start: number;
  col_start: number;
  n_rows: number;
  n_cols: number;
  columns: string[];
  rows: CellLike[][];
}

export interface PreviewDims {
  rows: number;
  cols: number;
}
