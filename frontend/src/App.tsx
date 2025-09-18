import { useEffect, useState } from "react";
import { healthz } from "./lib/api";

export default function App() {
  const [status, setStatus] = useState<string>("…");

  useEffect(() => {
    healthz()
      .then((j) => setStatus(j.status))
      .catch(() => setStatus("down"));
  }, []);

  return (
    <div className="min-h-screen grid place-items-center bg-gray-50">
      <div className="p-6 rounded-2xl shadow bg-white">
        <h1 className="text-2xl font-semibold mb-2">PandasQuiz — Dev</h1>
        <p className="text-sm text-gray-600">
          API health: <span className="font-mono">{status}</span>
        </p>
      </div>
    </div>
  );
}
