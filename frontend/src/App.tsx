// picks file, uploads to backend, renders a preview table

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { uploadDataset, listDatasets, getDataset } from "./lib/api";
import type { UploadInfo, DatasetListItem } from "./lib/types";
import ProfileView from "./components/ProfileView";
import QuestionsView from "./components/QuestionsView";
import QuizPlay from "./components/QuizPlay";
import PastFilesPage from "./components/PastFilesPage";
import UploadTab from "./components/UploadTab";
import EditorDrawer from "./components/EditorDrawer";
import SqlEditorPanel from "./components/SqlEditorPanel";
import PandasEditorPanel from "./components/PandasEditorPanel";

type Tab = "upload" | "profile" | "questions" | "quiz";
type Page = "home" | "past-files";
type EditorKind = "sql" | "pandas";

type EditorState = {
  sqlQuery: string;
  pandasCode: string;
};

const DEFAULT_SQL_QUERY = "SELECT * FROM data LIMIT 10;";
const DEFAULT_PANDAS_CODE =
  "# df is a pandas DataFrame built from the upload preview (first ~50 rows)\n\n" +
  "# Examples:\n" +
  "# print(df.head())\n" +
  "# print(df.columns)\n" +
  "# print(df.describe(include='all'))\n\n" +
  "# You can optionally set a variable named result to show something custom:\n" +
  "# result = df.head(10)\n";

function loadEditorState(): Record<string, EditorState> {
  try {
    const raw = localStorage.getItem("pandsqai.editorState.v1");
    if (!raw) return {};
    const parsed = JSON.parse(raw) as unknown;
    if (!parsed || typeof parsed !== "object") return {};
    return parsed as Record<string, EditorState>;
  } catch {
    return {};
  }
}

function saveEditorState(state: Record<string, EditorState>) {
  try {
    localStorage.setItem("pandsqai.editorState.v1", JSON.stringify(state));
  } catch {
    // ignore
  }
}

// user picks a CSV/Parquet file, shows a sample preview (50 lines), and can reveal the Profile/Question tabs once dataset is uploaded
export default function App() {
  const [info, setInfo] = useState<UploadInfo | null>(null); // holds server response (null before upload)
  const [err, setErr] = useState<string>(""); // human readable error message
  const [tab, setTab] = useState<Tab>("upload");
  const [page, setPage] = useState<Page>("home");
  const [editorOpen, setEditorOpen] = useState<EditorKind | null>(null);
  const [editorByDataset, setEditorByDataset] = useState<Record<string, EditorState>>(
    () => loadEditorState()
  );
  const [pastDatasets, setPastDatasets] = useState<DatasetListItem[]>([]);
  const [loadingPast, setLoadingPast] = useState(false);

  // toast (top-middle, auto-hide)
  const [toastMsg, setToastMsg] = useState<string | null>(null);

  const fileInputRef = useRef<HTMLInputElement>(null);

  const datasetId = info?.dataset_id ?? null;
  const activeEditorState: EditorState | null = useMemo(() => {
    if (!datasetId) return null;
    return (
      editorByDataset[datasetId] ?? {
        sqlQuery: DEFAULT_SQL_QUERY,
        pandasCode: DEFAULT_PANDAS_CODE,
      }
    );
  }, [datasetId, editorByDataset]);

  // auto-hide toast after 1 second
  useEffect(() => {
    if (!toastMsg) return;
    const t = window.setTimeout(() => setToastMsg(null), 1000);
    return () => window.clearTimeout(t);
  }, [toastMsg]);

  // when user picks a file in <input type="file">, function onChange runs
  async function onChange(e: React.ChangeEvent<HTMLInputElement>) {
    const f = e.target.files?.[0]; // handles empty files
    if (!f) return; // user cancels
    setErr(""); // clear error before next attempt

    // Snapshot existing dataset IDs BEFORE upload so we can detect dedup
    let existingIds = new Set<string>();
    try {
      const res = await listDatasets();
      existingIds = new Set(res.datasets.map((d) => d.dataset_id));
    } catch {
      // If this fails, we just skip the repeat-dataset toast.
      existingIds = new Set<string>();
    }

    try {
      const j = await uploadDataset(f); // post to get JSON back

      if (existingIds.has(j.dataset_id)) {
        // tiny 1-second notification (only relevant on upload page)
        setToastMsg("Repeat dataset");
      }

      setInfo(j);
      setEditorOpen(null);
    } catch (err: unknown) {
      const message = err instanceof Error ? err.message : String(err);
      setErr(message || "upload failed");
      setInfo(null); // clear data if error
    }
  }

  // clear the dataset and reset to initial state
  function clearDataset() {
    const current = info?.dataset_id;
    setInfo(null);
    setErr("");
    setTab("upload");
    setPage("home");
    setEditorOpen(null);
    if (current) {
      setEditorByDataset((prev) => {
        const next = { ...prev };
        delete next[current];
        saveEditorState(next);
        return next;
      });
    }
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
  const goToPastFiles = useCallback(async () => {
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
  }, []);

  // load a past dataset
  const loadPastDataset = useCallback(async (dataset_id: string) => {
    setErr("");
    try {
      const j = await getDataset(dataset_id);
      setInfo(j);
      setPage("home");
      setTab("upload");
      setEditorOpen(null);
    } catch (err: unknown) {
      const message = err instanceof Error ? err.message : String(err);
      setErr(message || "Failed to load dataset");
    }
  }, []);

  // go back to home from past files
  function goBackHome() {
    setPage("home");
    setErr("");
  }

  // Ensure every dataset gets an editor state bucket on first focus.
  useEffect(() => {
    if (!datasetId) return;
    setEditorByDataset((prev) => {
      if (prev[datasetId]) return prev;
      const next = {
        ...prev,
        [datasetId]: {
          sqlQuery: DEFAULT_SQL_QUERY,
          pandasCode: DEFAULT_PANDAS_CODE,
        },
      };
      saveEditorState(next);
      return next;
    });
  }, [datasetId]);

  // Persist editor state updates.
  useEffect(() => {
    saveEditorState(editorByDataset);
  }, [editorByDataset]);

  // Fetch past datasets when navigating to past-files page
  useEffect(() => {
    if (page !== "past-files") return;
    void goToPastFiles();
  }, [page, goToPastFiles]);

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

  // Render Home page
  return (
    <div className="min-h-screen p-6">
      {/* Top-mid toast (only shows when set) */}
      {toastMsg && tab === "upload" && (
        <div className="fixed top-4 left-1/2 -translate-x-1/2 z-60">
          <div className="px-3 py-2 text-sm bg-gray-800 text-white rounded shadow">
            {toastMsg}
          </div>
        </div>
      )}

      <div className="max-w-6xl mx-auto space-y-4">
        <div className="flex justify-between items-center gap-2">
          <h1 className="text-2xl font-semibold">Upload dataset</h1>
          <div className="flex items-center gap-2">
            {info && (
              <>
                <button
                  onClick={() => setEditorOpen("sql")}
                  className="px-4 py-2 bg-gray-600 text-white rounded hover:bg-gray-700 active:bg-gray-800 transition-colors"
                >
                  SQL editor
                </button>
                <button
                  onClick={() => setEditorOpen("pandas")}
                  className="px-4 py-2 bg-gray-600 text-white rounded hover:bg-gray-700 active:bg-gray-800 transition-colors"
                >
                  Pandas editor
                </button>
              </>
            )}
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
            // ✅ only allow horizontal scrolling when an editor is open
            allowHorizontalScroll={!!editorOpen}
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

      <EditorDrawer
        open={!!editorOpen}
        title={
          editorOpen === "sql"
            ? "SQL editor"
            : editorOpen === "pandas"
              ? "Pandas editor"
              : "Editor"
        }
        subtitle={info ? info.display_name : undefined}
        onClose={() => setEditorOpen(null)}
      >
        {editorOpen === "sql" && activeEditorState && (
          <SqlEditorPanel
            dataset_id={datasetId}
            query={activeEditorState.sqlQuery}
            onQueryChange={(nextQuery) => {
              if (!datasetId) return;
              setEditorByDataset((prev) => ({
                ...prev,
                [datasetId]: {
                  ...(prev[datasetId] ?? {
                    sqlQuery: DEFAULT_SQL_QUERY,
                    pandasCode: DEFAULT_PANDAS_CODE,
                  }),
                  sqlQuery: nextQuery,
                },
              }));
            }}
          />
        )}

        {editorOpen === "pandas" && activeEditorState && (
          <PandasEditorPanel
            dataset_id={datasetId}
            code={activeEditorState.pandasCode}
            onCodeChange={(nextCode) => {
              if (!datasetId) return;
              setEditorByDataset((prev) => ({
                ...prev,
                [datasetId]: {
                  ...(prev[datasetId] ?? {
                    sqlQuery: DEFAULT_SQL_QUERY,
                    pandasCode: DEFAULT_PANDAS_CODE,
                  }),
                  pandasCode: nextCode,
                },
              }));
            }}
          />
        )}
      </EditorDrawer>
    </div>
  );
}
