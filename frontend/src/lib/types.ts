export type Cell = string | number | boolean | null | undefined; // data values expect to see in table cell


/**
 * Shape of thing /upload endpoint returns in python backend:
 * {
 *   dataset_id: "uuid",
 *   n_rows: 123,
 *   n_cols: 5,
 *   columns: ["a", "b", ...],
 *   sample: [{a: 1, b: 2}, {a: 3, b: 4}, ...]  // first 50 rows
 * }
**/

export interface UploadInfo {
  dataset_id: string;
  n_rows: number;
  n_cols: number;
  columns: string[];
  sample: Array<Record<string, Cell>>;
}

export interface TopKEntry {
  value: Cell;
  count: number;
}

export interface ColumnProfile {
  name: string;
  dtype: string;
  null_count: number;
  unique_count: number;
  examples: Cell[];
  min?: number;
  mean?: number;
  max?: number;
  std?: number;
  min_ts?: string;
  max_ts?: string;
  top_k?: TopKEntry[];
}

export interface ProfileInfo {
  n_rows: number;
  n_cols: number;
  columns: ColumnProfile[];
  features: {
    has_numeric: boolean;
    has_datetime: boolean;
    has_categorical: boolean;
    pivot_candidates: Array<[string, string]>;
    wide_to_long_candidates: string[];
  };
}