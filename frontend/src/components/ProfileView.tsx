import { useEffect, useState } from "react";
import { getProfile } from "../lib/api";
import type { ProfileInfo } from "../lib/types";

interface ProfileViewProps {
  dataset_id: string;
}

// fetches dataset profile from backend, then renders basic counts, table of per-column states, and pivot candidates
export default function ProfileView({ dataset_id }: ProfileViewProps) {
  const [profile, setProfile] = useState<ProfileInfo | null>(null); // loaded data
  const [err, setErr] = useState<string>("");
  const [loading, setLoading] = useState<boolean>(true); // while fetching, set this

  useEffect(() => {
    setLoading(true);
    setErr("");
    
    getProfile(dataset_id)
      .then((p) => {
        setProfile(p);
        setLoading(false);
      })
      .catch((error: unknown) => {
        const message = error instanceof Error ? error.message : String(error);
        setErr(message || "Failed to load profile");
        setProfile(null);
        setLoading(false);
      });
  }, [dataset_id]);

  if (loading) return <p className="text-gray-600">Loading profile...</p>;
  if (err) return <p className="text-red-600">{err}</p>;
  if (!profile) return null;

  const { n_rows, n_cols, columns, features } = profile;

  return (
    <div className="space-y-4">
      <div className="space-y-1">
        <p className="text-sm text-gray-700">
          <b>Rows:</b> {n_rows.toLocaleString()} • <b>Cols:</b> {n_cols}
        </p>
        <p className="text-sm text-gray-700">
          <b>Has Numeric:</b> {features.has_numeric ? "Yes" : "No"} • 
          <b> Has Datetime:</b> {features.has_datetime ? "Yes" : "No"} • 
          <b> Has Categorical:</b> {features.has_categorical ? "Yes" : "No"}
        </p>
      </div>

      <div className="overflow-auto border rounded">
        <table className="min-w-full text-sm">
          <thead>
            <tr className="bg-gray-100">
              <th className="px-2 py-1 text-left border-b">Name</th>
              <th className="px-2 py-1 text-left border-b">Type</th>
              <th className="px-2 py-1 text-left border-b">% Null</th>
              <th className="px-2 py-1 text-left border-b"># Unique</th>
              <th className="px-2 py-1 text-left border-b">Examples</th>
              <th className="px-2 py-1 text-left border-b">Stats</th>
            </tr>
          </thead>
          <tbody>
            {columns.map((col) => {
              const pct_null = n_rows > 0 ? ((col.null_count / n_rows) * 100).toFixed(1) : "0.0";
              
              return (
                <tr key={col.name} className="odd:bg-gray-50">
                  <td className="px-2 py-1 border-b font-mono text-xs">{col.name}</td>
                  <td className="px-2 py-1 border-b text-xs">{col.dtype}</td>
                  <td className="px-2 py-1 border-b text-xs">{pct_null}%</td>
                  <td className="px-2 py-1 border-b text-xs">{col.unique_count.toLocaleString()}</td>
                  <td className="px-2 py-1 border-b text-xs">
                    {col.examples.map((ex, i) => (
                      <span key={i}>
                        {String(ex ?? "null")}
                        {i < col.examples.length - 1 ? ", " : ""}
                      </span>
                    ))}
                  </td>
                  <td className="px-2 py-1 border-b text-xs">
                    {col.min !== undefined && (
                      <div>min: {col.min}, mean: {col.mean}, max: {col.max}, std: {col.std}</div>
                    )}
                    {col.min_ts && (
                      <div>{col.min_ts} → {col.max_ts}</div>
                    )}
                    {col.top_k && (
                      <div className="space-y-0.5">
                        {col.top_k.map((tk, i) => (
                          <div key={i}>
                            {String(tk.value ?? "null")}: {tk.count}
                          </div>
                        ))}
                      </div>
                    )}
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>

      {features.pivot_candidates.length > 0 && (
        <div className="text-sm text-gray-700">
          <b>Pivot Candidates:</b> {features.pivot_candidates.map(([a, b]) => `${a} × ${b}`).join(", ")}
        </div>
      )}
    </div>
  );
}