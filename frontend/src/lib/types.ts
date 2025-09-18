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
