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

export interface MultiSqlRequest {
  query: string;
  max_cells?: number;
  max_rows?: number;
}

export interface MultiSqlResponse {
  columns: string[];
  rows: CellLike[][];
  truncated: boolean;
  note: string | null;
}

export interface PreviewDims {
  rows: number;
  cols: number;
}

export type OpsMergeHow = "inner" | "left" | "right" | "outer";

export type OpsTable = "t1" | "t2" | "t3";

export type OpsCmp = "==" | "!=" | "<" | "<=" | ">" | ">=";

export type OpsAggFn = "sum" | "avg" | "count" | "min" | "max";

export type OpsStep =
  | { op: "source"; table: OpsTable }
  | { op: "select"; columns: string[] }
  | {
      op: "filter";
      conditions: Array<{ column: string; cmp: OpsCmp; value: CellLike }>;
    }
  | {
      op: "merge";
      right_table: OpsTable;
      how: "inner" | "left" | "right" | "outer";
      left_on: string[];
      right_on: string[];
    }
  | {
      op: "groupby";
      by: string[];
      aggs: Array<{ column: string; fn: OpsAggFn; as: string }>;
    }
  | { op: "sort"; by: string[]; ascending?: boolean[] }
  | { op: "limit"; n: number };

export type MultiOpsRequest = {
  steps: OpsStep[];
  max_cells?: number;
  max_rows?: number;
};

export type MultiOpsResponse = {
  columns: string[];
  rows: CellLike[][];
  truncated: boolean;
  note: string | null;
};
