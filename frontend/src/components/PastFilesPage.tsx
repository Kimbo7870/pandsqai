import type { DatasetListItem } from "../lib/types";

interface Props {
  err: string;
  loadingPast: boolean;
  pastDatasets: DatasetListItem[];
  onBack: () => void;
  onLoad: (dataset_id: string) => void;
}

export default function PastFilesPage({
  err,
  loadingPast,
  pastDatasets,
  onBack,
  onLoad,
}: Props) {
  return (
    <div className="min-h-screen p-6">
      <div className="max-w-6xl mx-auto space-y-4">
        <div className="flex justify-between items-center">
          <h1 className="text-2xl font-semibold">Past Files</h1>
          <button
            onClick={onBack}
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
                  <th className="px-4 py-2 text-left border-b border-gray-300">
                    File Name
                  </th>
                  <th className="px-4 py-2 text-left border-b border-gray-300">
                    Rows
                  </th>
                  <th className="px-4 py-2 text-left border-b border-gray-300">
                    Cols
                  </th>
                  <th className="px-4 py-2 text-left border-b border-gray-300">
                    Action
                  </th>
                </tr>
              </thead>
              <tbody>
                {pastDatasets.map((ds) => (
                  <tr
                    key={ds.dataset_id}
                    className="odd:bg-gray-50 hover:bg-gray-100"
                  >
                    <td className="px-4 py-2 border-b border-gray-300">
                      {ds.display_name}
                    </td>
                    <td className="px-4 py-2 border-b border-gray-300">
                      {ds.n_rows.toLocaleString()}
                    </td>
                    <td className="px-4 py-2 border-b border-gray-300">
                      {ds.n_cols}
                    </td>
                    <td className="px-4 py-2 border-b border-gray-300">
                      <button
                        onClick={() => onLoad(ds.dataset_id)}
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
