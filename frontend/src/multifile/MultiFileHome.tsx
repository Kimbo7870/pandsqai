import { Link } from "react-router-dom";

export default function MultiFileHome() {
  return (
    <div className="min-h-screen bg-gray-50">
      <div className="max-w-5xl mx-auto p-6 space-y-6">
        <div className="flex items-center justify-between">
          <h1 className="text-2xl font-semibold text-gray-900">Multifile</h1>
          <Link
            to="/"
            className="px-4 py-2 bg-gray-600 text-white rounded hover:bg-gray-700 active:bg-gray-800 transition-colors"
          >
            Back
          </Link>
        </div>

        <div className="border border-gray-300 rounded p-4 bg-white">
          <div className="flex items-center justify-between">
            <div>
              <p className="font-medium text-gray-900">Upload datasets (0–3)</p>
              <p className="text-sm text-gray-600">
                Placeholder — upload, persistence, and previews are coming in later chunks.
              </p>
            </div>
            <button
              disabled
              className="px-4 py-2 bg-gray-300 text-gray-700 rounded cursor-not-allowed"
            >
              Upload
            </button>
          </div>
        </div>

        <div className="border border-gray-300 rounded p-4 bg-white">
          <div className="flex items-center justify-between">
            <p className="font-medium text-gray-900">Current datasets</p>
            <button
              disabled
              className="px-4 py-2 bg-gray-300 text-gray-700 rounded cursor-not-allowed"
            >
              Delete all
            </button>
          </div>
          <p className="mt-3 text-sm text-gray-600">No datasets loaded yet.</p>
        </div>

        <div className="flex gap-2">
          <Link
            to="/multifile/view-all"
            className="px-4 py-2 bg-gray-600 text-white rounded hover:bg-gray-700 active:bg-gray-800 transition-colors"
          >
            View all
          </Link>
        </div>
      </div>
    </div>
  );
}
