// picks file, uploads to backend, renders a preview table

import { useEffect, useRef, useState } from "react";
import { uploadDataset, listDatasets, getDataset } from "./lib/api";
import type { UploadInfo, DatasetListItem } from "./lib/types";
import ProfileView from "./components/ProfileView";
import QuestionsView from "./components/QuestionsView";
import QuizPlay from "./components/QuizPlay";
import PastFilesPage from "./components/PastFilesPage";
import UploadTab from "./components/UploadTab";
import SqlEditorPage from "./components/SqlEditorPage";
import PandasEditorPage from "./components/PandasEditorPage";

type Tab = "upload" | "profile" | "questions" | "quiz";
type Page = "home" | "past-files" | "sql-editor" | "pandas-editor";

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
      <PastFilesPage
        err={err}
        loadingPast={loadingPast}
        pastDatasets={pastDatasets}
        onBack={goBackHome}
        onLoad={loadPastDataset}
      />
    );
  }

  if (page === "sql-editor") {
    return (
      <SqlEditorPage
        dataset_id={info ? info.dataset_id : null}
        onBack={goBackHome}
      />
    );
  }

  if (page === "pandas-editor") {
    return (
      <PandasEditorPage
        dataset_id={info ? info.dataset_id : null}
        onBack={goBackHome}
      />
    );
  }

  // Render Home page
  return (
    <div className="min-h-screen p-6">
      <div className="max-w-6xl mx-auto space-y-4">
        <div className="flex justify-between items-center gap-2">
          <h1 className="text-2xl font-semibold">Upload dataset</h1>
          <div className="flex items-center gap-2">
            <button
              onClick={() => setPage("sql-editor")}
              disabled={!info}
              className="px-4 py-2 bg-gray-600 text-white rounded hover:bg-gray-700 active:bg-gray-800 transition-colors disabled:bg-gray-400"
            >
              SQL editor
            </button>
            <button
              onClick={() => setPage("pandas-editor")}
              disabled={!info}
              className="px-4 py-2 bg-gray-600 text-white rounded hover:bg-gray-700 active:bg-gray-800 transition-colors disabled:bg-gray-400"
            >
              Pandas editor
            </button>
            {info && (
              <button
                onClick={clearDataset}
                className="px-4 py-2 bg-gray-600 text-white rounded hover:bg-gray-700 active:bg-gray-800 transition-colors"
              >
                Clear File
              </button>
            )}
          </div>
        </div>

        {info && (
          <div className="flex gap-2 border-b border-gray-300">
            <button
              onClick={() => setTab("upload")}
              className={`px-4 py-2 ${
                tab === "upload"
                  ? "border-b-2 border-gray-600 font-semibold"
                  : "text-gray-600"
              }`}
            >
              Upload
            </button>
            <button
              onClick={() => setTab("profile")}
              className={`px-4 py-2 ${
                tab === "profile"
                  ? "border-b-2 border-gray-600 font-semibold"
                  : "text-gray-600"
              }`}
            >
              Profile
            </button>
            <button
              onClick={() => setTab("questions")}
              className={`px-4 py-2 ${
                tab === "questions"
                  ? "border-b-2 border-gray-600 font-semibold"
                  : "text-gray-600"
              }`}
            >
              Questions
            </button>
            <button
              onClick={() => setTab("quiz")}
              className={`px-4 py-2 ${
                tab === "quiz"
                  ? "border-b-2 border-gray-600 font-semibold"
                  : "text-gray-600"
              }`}
            >
              Quiz
            </button>
          </div>
        )}

        {tab === "upload" && (
          <UploadTab
            info={info}
            err={err}
            loadingPast={loadingPast}
            fileInputRef={fileInputRef}
            onChange={onChange}
            handleChooseFile={handleChooseFile}
            goToPastFiles={goToPastFiles}
          />
        )}

        {tab === "profile" && info && <ProfileView dataset_id={info.dataset_id} />}

        {tab === "questions" && info && <QuestionsView dataset_id={info.dataset_id} />}

        {tab === "quiz" && info && (
          <div className="mt-4">
            <h2 className="text-lg font-semibold mb-2">Quiz Mode</h2>
            <QuizPlay dataset_id={info.dataset_id} limit={8} seed={0} />
          </div>
        )}
      </div>
    </div>
  );
}
