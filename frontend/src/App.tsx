// picks file, uploads to backend, renders a preview table

import { useState } from "react";
import { uploadDataset } from "./lib/api";
import type { UploadInfo, Cell } from "./lib/types";

export default function App() {
  const [info, setInfo] = useState<UploadInfo | null>(null); // holds server response (null before upload)
  const [err, setErr] = useState<string>(""); // human readable error message

  // when user picks a file in <input type="file">, function onChange runs
  async function onChange(e: React.ChangeEvent<HTMLInputElement>) {
    const f = e.target.files?.[0]; // handles empty files
    if (!f) return; // user cancels
    setErr(""); // clear error before next attempt
    try {
      const j = await uploadDataset(f); // post to get JSON back
      setInfo(j);
    } catch (err: unknown) {
      const message = err instanceof Error ? err.message : String(err);
      setErr(message || "upload failed");
      setInfo(null); // clear data if error
    }
  }

  return (
    <div className="min-h-screen p-6">
      <div className="max-w-4xl mx-auto space-y-4">
        <h1 className="text-2xl font-semibold">Upload dataset</h1>

        <input type="file" accept=".csv,.parquet" onChange={onChange} /> {/* pick file*/}

        {err && <p className="text-red-600">{err}</p>} {/* error */}

        {/* render sample if info state is set */}
        {info && (
          <div className="space-y-2">
            <p className="text-sm text-gray-700">
              <b>ID:</b> {info.dataset_id} • <b>Rows:</b> {info.n_rows} • <b>Cols:</b> {info.n_cols}
            </p>

            {/* render and display */}
            <div className="overflow-auto border rounded">
              <table className="min-w-full text-sm">
                <thead>
                  <tr>
                    {info.columns.map((c) => (
                      <th key={c} className="px-2 py-1 text-left border-b">
                        {c}
                      </th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {info.sample.map((row, i) => (
                    <tr key={i} className="odd:bg-gray-50">
                      {info.columns.map((c) => {
                        // Because `row` is Record<string, Cell>, accessing row[c] is safe and typed.
                        const v: Cell = row[c];
                        // String(v ?? "") ensures:
                        // - numbers/booleans stringify nicely
                        // - null/undefined become ""
                        return (
                          <td key={c} className="px-2 py-1 border-b">
                            {String(v ?? "")}
                          </td>
                        );
                      })}
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}