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

// defining exact shapes of data frontend expects from backend

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


export type QuestionType =
  | "col_dtype_id"
  | "col_missing_pct"
  | "col_unique_count"
  | "col_topk_value"
  | "col_numeric_min"
  | "col_numeric_max"
  | "col_numeric_mean"
  | "col_date_range";

export type DtypeAnswer = "integer" | "float" | "boolean" | "datetime" | "string";

/** Use a real object constraint + empty-object default */
interface BaseQuestion<
  TType extends QuestionType,
  TAnswer,
  TMeta extends Record<string, unknown> = Record<string, never>
> {
  id: string;
  type: TType;
  prompt: string;
  choices?: string[];
  answer: TAnswer;
  rationale?: string;
  metadata: TMeta;
}

/** Per-type specializations */
export type QColDtypeId = BaseQuestion<
  "col_dtype_id",
  DtypeAnswer,
  { column: string }
>;

export type QColMissingPct = BaseQuestion<
  "col_missing_pct",
  number,
  { column: string; round: 1 }
>;

export type QColUniqueCount = BaseQuestion<
  "col_unique_count",
  number,
  { column: string }
>;

export type QColTopKValue = BaseQuestion<
  "col_topk_value",
  string,
  { column: string; k: number }
>;

export type QColNumericMin = BaseQuestion<
  "col_numeric_min",
  number,
  { column: string }
>;

export type QColNumericMax = BaseQuestion<
  "col_numeric_max",
  number,
  { column: string }
>;

export type QColNumericMean = BaseQuestion<
  "col_numeric_mean",
  number,
  { column: string; round: 2 }
>;

export type QColDateRange = BaseQuestion<
  "col_date_range",
  { min: string; max: string },
  { column: string }
>;


export type Question =
  | QColDtypeId
  | QColMissingPct
  | QColUniqueCount
  | QColTopKValue
  | QColNumericMin
  | QColNumericMax
  | QColNumericMean
  | QColDateRange;

export interface QuestionsResponse {
  dataset_id: string;
  seed: number;
  count: number;
  questions: Question[];
}
