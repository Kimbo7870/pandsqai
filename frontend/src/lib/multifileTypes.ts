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

export interface PreviewDims {
  rows: number;
  cols: number;
}
