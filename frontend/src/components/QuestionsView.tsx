import { useEffect, useMemo, useState } from "react";
import { getQuestions } from "../lib/api";
import type { QuestionsResponse, Question } from "../lib/types";

interface Props {
  dataset_id: string;
}

function renderAnswer(q: Question): string {
  switch (q.type) {
    case "col_dtype_id":
      return q.answer; // DtypeAnswer
    case "col_missing_pct":
    case "col_unique_count":
    case "col_numeric_min":
    case "col_numeric_max":
    case "col_numeric_mean":
      return String(q.answer); // number
    case "col_topk_value":
      return q.answer; // string (value stringified on server)
    case "col_date_range":
      return `${q.answer.min} → ${q.answer.max}`; // ISO strings
    default: {
      // ensures compile-time exhaustiveness if a new type is added
      const _exhaustive: never = q;
      return String(_exhaustive);
    }
  }
}

// lets user set a seed (for deterministicness) and limit (number of questions), fetches, then shows table of questions
export default function QuestionsView({ dataset_id }: Props) {
  const [seed, setSeed] = useState<number>(0);
  const [limit, setLimit] = useState<number>(12);
  const [data, setData] = useState<QuestionsResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [err, setErr] = useState("");

  // don't fetch when dataset is not present or limit is <= 0
  const canFetch = useMemo(() => dataset_id && limit > 0, [dataset_id, limit]);

  async function fetchQuestions() {
    if (!canFetch) return;
    setLoading(true);
    setErr("");
    try {
      const q = await getQuestions(dataset_id, limit, seed);
      setData(q);
    } catch (e: unknown) {
      setData(null);
      setErr(e instanceof Error ? e.message : String(e));
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    fetchQuestions();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [dataset_id]);

  return (
    <div className="space-y-4">
      <div className="flex items-end gap-4">
        <div>
          <label className="block text-xs text-gray-600">Seed</label>
          <input
            type="number"
            value={seed}
            onChange={(e) => setSeed(parseInt(e.target.value || "0", 10))}
            className="border rounded px-2 py-1 text-sm w-28"
          />
        </div>
        <div>
          <label className="block text-xs text-gray-600">Limit</label>
          <input
            type="number"
            min={1}
            max={64}
            value={limit}
            onChange={(e) => setLimit(parseInt(e.target.value || "12", 10))}
            className="border rounded px-2 py-1 text-sm w-28"
          />
        </div>
        <button
          onClick={fetchQuestions}
          className="px-3 py-2 rounded bg-blue-600 text-white text-sm shadow"
          disabled={loading || !canFetch}
        >
          {loading ? "Generating…" : "Generate"}
        </button>
        {err && <span className="text-red-600 text-sm">{err}</span>}
      </div>

      {data && (
        <div className="space-y-2">
          <p className="text-sm text-gray-700">
            <b>Seed:</b> {data.seed} • <b>Count:</b> {data.count}
          </p>
          <div className="overflow-auto border rounded">
            <table className="min-w-full text-sm">
              <thead>
                <tr className="bg-gray-100">
                  <th className="px-2 py-1 text-left border-b">Type</th>
                  <th className="px-2 py-1 text-left border-b">Prompt</th>
                  <th className="px-2 py-1 text-left border-b">Choices</th>
                  <th className="px-2 py-1 text-left border-b">Answer</th>
                  <th className="px-2 py-1 text-left border-b">Meta</th>
                </tr>
              </thead>
              <tbody>
                {data.questions.map((q: Question) => (
                  <tr key={q.id} className="odd:bg-gray-50 align-top">
                    <td className="px-2 py-1 border-b text-xs font-mono">{q.type}</td>
                    <td className="px-2 py-1 border-b text-xs">{q.prompt}</td>
                    <td className="px-2 py-1 border-b text-xs">
                      {"choices" in q && q.choices ? q.choices.join(", ") : "—"}
                    </td>
                    <td className="px-2 py-1 border-b text-xs">{renderAnswer(q)}</td>
                    <td className="px-2 py-1 border-b text-xs">
                      {JSON.stringify(q.metadata)}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}
    </div>
  );
}
