// picks file, uploads to backend, renders a preview table

import { useEffect, useRef, useState } from "react";
import { uploadDataset, listDatasets, getDataset } from "./lib/api";
import type { UploadInfo, Cell, DatasetListItem } from "./lib/types";
import ProfileView from "./components/ProfileView";
import QuestionsView from "./components/QuestionsView";
import QuizPlay from "./components/QuizPlay";

type Tab = "upload" | "profile" | "questions" | "quiz";
type Page = "home" | "past-files";

// user picks a CSV/Parquet file, shows a sample preview (50 lines), and can reveal the Profile/Question tabs once dataset is uploaded
export default function App() {
  const [info, setInfo] = useState<UploadInfo | null>(null); // holds server response (null before upload)
  const [err, setErr] = useState<string>(""); // human readable error message
  const [tab, setTab] = useState<Tab>("upload");
  const [page, setPage] = useState<Page>("home");
  const [pastDatasets, setPastDatasets] = useState<DatasetListItem[]>([]);
  const [loadingPast, setLoadingPast] = useState(false);
  const fileInputRef = useRef<HTMLInputElement>(null);

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

  // clear the dataset and reset to initial state
  function clearDataset() {
    setInfo(null);
    setErr("");
    setTab("upload");
    setPage("home");
    // Reset the file input so the same file can be re-selected
    if (fileInputRef.current) {
      fileInputRef.current.value = "";
    }
  }

  // trigger the hidden file input when button is clicked
  function handleChooseFile() {
    fileInputRef.current?.click();
  }

  // navigate to past files page
  async function goToPastFiles() {
    setLoadingPast(true);
    setErr("");
    try {
      const res = await listDatasets();
      setPastDatasets(res.datasets);
      setPage("past-files");
    } catch (err: unknown) {
      const message = err instanceof Error ? err.message : String(err);
      setErr(message || "Failed to load past files");
    } finally {
      setLoadingPast(false);
    }
  }

  // load a past dataset
  async function loadPastDataset(dataset_id: string) {
    setErr("");
    try {
      const j = await getDataset(dataset_id);
      setInfo(j);
      setPage("home");
      setTab("upload");
    } catch (err: unknown) {
      const message = err instanceof Error ? err.message : String(err);
      setErr(message || "Failed to load dataset");
    }
  }

  // go back to home from past files
  function goBackHome() {
    setPage("home");
    setErr("");
  }

  // Fetch past datasets when navigating to past-files page
  useEffect(() => {
    if (page === "past-files") {
      goToPastFiles();
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  // Render Past Files page
  if (page === "past-files") {
    return (
      <div className="min-h-screen p-6">
        <div className="max-w-6xl mx-auto space-y-4">
          <div className="flex justify-between items-center">
            <h1 className="text-2xl font-semibold">Past Files</h1>
            <button
              onClick={goBackHome}
              className="px-4 py-2 bg-gray-600 text-white rounded hover:bg-gray-700 active:bg-gray-800 transition-colors"
            >
              Back
            </button>
          </div>

          {err && <p className="text-gray-700">{err}</p>}

          {loadingPast ? (
            <p className="text-gray-600">Loading past files...</p>
          ) : pastDatasets.length === 0 ? (
            <p className="text-gray-600">No past files found.</p>
          ) : (
            <div className="overflow-auto border border-gray-300 rounded">
              <table className="min-w-full text-sm">
                <thead>
                  <tr className="bg-gray-100">
                    <th className="px-4 py-2 text-left border-b border-gray-300">File Name</th>
                    <th className="px-4 py-2 text-left border-b border-gray-300">Rows</th>
                    <th className="px-4 py-2 text-left border-b border-gray-300">Cols</th>
                    <th className="px-4 py-2 text-left border-b border-gray-300">Action</th>
                  </tr>
                </thead>
                <tbody>
                  {pastDatasets.map((ds) => (
                    <tr key={ds.dataset_id} className="odd:bg-gray-50 hover:bg-gray-100">
                      <td className="px-4 py-2 border-b border-gray-300">{ds.display_name}</td>
                      <td className="px-4 py-2 border-b border-gray-300">{ds.n_rows.toLocaleString()}</td>
                      <td className="px-4 py-2 border-b border-gray-300">{ds.n_cols}</td>
                      <td className="px-4 py-2 border-b border-gray-300">
                        <button
                          onClick={() => loadPastDataset(ds.dataset_id)}
                          className="px-3 py-1 bg-gray-600 text-white rounded hover:bg-gray-700 active:bg-gray-800 transition-colors text-sm"
                        >
                          Load
                        </button>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </div>
      </div>
    );
  }

  // Render Home page
  return (
    <div className="min-h-screen p-6">
      <div className="max-w-6xl mx-auto space-y-4">
        <div className="flex justify-between items-center">
          <h1 className="text-2xl font-semibold">Upload dataset</h1>
          {info && (
            <button
              onClick={clearDataset}
              className="px-4 py-2 bg-gray-600 text-white rounded hover:bg-gray-700 active:bg-gray-800 transition-colors"
            >
              Clear File
            </button>
          )}
        </div>

        {info && (
          <div className="flex gap-2 border-b border-gray-300">
            <button
              onClick={() => setTab("upload")}
              className={`px-4 py-2 ${tab === "upload" ? "border-b-2 border-gray-600 font-semibold" : "text-gray-600"}`}
            >
              Upload
            </button>
            <button
              onClick={() => setTab("profile")}
              className={`px-4 py-2 ${tab === "profile" ? "border-b-2 border-gray-600 font-semibold" : "text-gray-600"}`}
            >
              Profile
            </button>
            <button
              onClick={() => setTab("questions")}
              className={`px-4 py-2 ${tab === "questions" ? "border-b-2 border-gray-600 font-semibold" : "text-gray-600"}`}
            >
              Questions
            </button>
            <button
              onClick={() => setTab("quiz")}
              className={`px-4 py-2 ${tab === "quiz" ? "border-b-2 border-gray-600 font-semibold" : "text-gray-600"}`}
            >
              Quiz
            </button>
          </div>
        )}

        {tab === "upload" && (
          <>
            {/* Hidden file input */}
            <input
              ref={fileInputRef}
              type="file"
              accept=".csv,.parquet"
              onChange={onChange}
              className="hidden"
            />
            {/* Styled buttons - only shown when no dataset is loaded */}
            {!info && (
              <div className="flex gap-2">
                <button
                  onClick={handleChooseFile}
                  className="px-4 py-2 bg-gray-600 text-white rounded hover:bg-gray-700 active:bg-gray-800 transition-colors"
                >
                  Choose File
                </button>
                <button
                  onClick={goToPastFiles}
                  disabled={loadingPast}
                  className="px-4 py-2 bg-gray-600 text-white rounded hover:bg-gray-700 active:bg-gray-800 transition-colors disabled:bg-gray-400"
                >
                  {loadingPast ? "Loading..." : "Past Files"}
                </button>
              </div>
            )}

            {err && <p className="text-gray-700">{err}</p>}

            {/* render sample if info state is set */}
            {info && (
              <div className="space-y-2">
                <p className="text-sm text-gray-700">
                  <b>File:</b> {info.display_name} • <b>Rows:</b> {info.n_rows} • <b>Cols:</b> {info.n_cols}
                </p>

                {/* render and display */}
                <div className="overflow-auto border border-gray-300 rounded">
                  <table className="min-w-full text-sm">
                    <thead>
                      <tr>
                        {info.columns.map((c) => (
                          <th key={c} className="px-2 py-1 text-left border-b border-gray-300">
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
                              <td key={c} className="px-2 py-1 border-b border-gray-300">
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
          </>
        )}

        {tab === "profile" && info && <ProfileView dataset_id={info.dataset_id} />}

        {tab === "questions" && info && <QuestionsView dataset_id={info.dataset_id} />}

        {tab === "quiz" && info && (
          <div className="mt-4">
            <h2 className="text-lg font-semibold mb-2">Quiz Mode</h2>
            <QuizPlay dataset_id={info.dataset_id} limit={8} seed={0} />
          </div>)}
      </div>
    </div>
  );
}