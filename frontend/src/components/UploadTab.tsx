import type { UploadInfo, Cell } from "../lib/types";

interface Props {
  info: UploadInfo | null;
  err: string;
  loadingPast: boolean;
  fileInputRef: React.RefObject<HTMLInputElement | null>;
  onChange: (e: React.ChangeEvent<HTMLInputElement>) => Promise<void>;
  handleChooseFile: () => void;
  goToPastFiles: () => Promise<void>;
}

export default function UploadTab({
  info,
  err,
  loadingPast,
  fileInputRef,
  onChange,
  handleChooseFile,
  goToPastFiles,
}: Props) {
  return (
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
            <b>File:</b> {info.display_name} • <b>Rows:</b> {info.n_rows} •{" "}
            <b>Cols:</b> {info.n_cols}
          </p>

          {/* render and display */}
          <div className="overflow-auto border border-gray-300 rounded">
            <table className="min-w-full text-sm">
              <thead>
                <tr>
                  {info.columns.map((c) => (
                    <th
                      key={c}
                      className="px-2 py-1 text-left border-b border-gray-300"
                    >
                      {c}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {info.sample.map((row, i) => (
                  <tr key={i} className="odd:bg-gray-50">
                    {info.columns.map((c) => {
                      const v: Cell = row[c];
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
  );
}
