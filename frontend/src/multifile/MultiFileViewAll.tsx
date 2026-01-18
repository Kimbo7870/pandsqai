import { Link } from "react-router-dom";

export default function MultiFileViewAll() {
  return (
    <div className="min-h-screen bg-gray-50">
      <div className="max-w-5xl mx-auto p-6 space-y-6">
        <div className="flex items-center justify-between">
          <h1 className="text-2xl font-semibold text-gray-900">Multifile — View All</h1>
          <div className="flex gap-2">
            <Link
              to="/multifile"
              className="px-4 py-2 bg-gray-600 text-white rounded hover:bg-gray-700 active:bg-gray-800 transition-colors"
            >
              Back to Multifile Home
            </Link>
            <Link
              to="/"
              className="px-4 py-2 bg-gray-600 text-white rounded hover:bg-gray-700 active:bg-gray-800 transition-colors"
            >
              Back to Single-File
            </Link>
          </div>
        </div>

        <div className="border border-gray-300 rounded p-4 bg-white">
          <p className="font-medium text-gray-900">Placeholder layout</p>
          <p className="text-sm text-gray-600 mt-1">
            This page will render 1/2/3 fixed preview windows with click+drag panning in later chunks.
          </p>
        </div>
      </div>
    </div>
  );
}
