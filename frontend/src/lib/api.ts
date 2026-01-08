// API helper functions to connect to FastAPI backend, will have fetch logic

import type { UploadInfo, ProfileInfo, QuestionsResponse, DatasetsResponse } from "./types";

export const API_BASE = import.meta.env.VITE_API_BASE ?? "http://localhost:8000";


// sends a CSV or Parquet (multipart/form-data) to POST /upload
// multipart/form-data is HTTP content type for HTML forms that include files
export async function uploadDataset(file: File): Promise<UploadInfo> {
  const fd = new FormData(); // create multipart/form-data object
  fd.append("file", file); // key name file must match FastAPI param
  const res = await fetch(`${API_BASE}/upload`, { method: "POST", body: fd }); // POST request

  if (!res.ok) {
    const msg = await res.text().catch(() => "");
    throw new Error(msg || `Upload failed with ${res.status}`);
  }

  return res.json() as Promise<UploadInfo>; // Type assertion is dev-time, care, trusts backend matches UploadInfo.
}

// GET to /profile?dataset_id.... (usage is to fill table, used in ProfileView)
export async function getProfile(dataset_id: string): Promise<ProfileInfo> {
  const res = await fetch(`${API_BASE}/profile?dataset_id=${dataset_id}`);
  
  if (!res.ok) {
    const msg = await res.text().catch(() => "");
    throw new Error(msg || `Profile failed with ${res.status}`);
  }

  return res.json() as Promise<ProfileInfo>;
}

// used in QuestionView to get wuestions
export async function getQuestions(
  dataset_id: string,
  limit = 12,
  seed = 0
): Promise<QuestionsResponse> {
  const url = `${API_BASE}/questions?dataset_id=${encodeURIComponent(
    dataset_id
  )}&limit=${limit}&seed=${seed}`;
  const res = await fetch(url);
  if (!res.ok) {
    const msg = await res.text().catch(() => "");
    throw new Error(msg || `Questions failed with ${res.status}`);
  }
  return res.json() as Promise<QuestionsResponse>;
}

// GET /datasets - list all past datasets
export async function listDatasets(): Promise<DatasetsResponse> {
  const res = await fetch(`${API_BASE}/datasets`);
  if (!res.ok) {
    const msg = await res.text().catch(() => "");
    throw new Error(msg || `Failed to list datasets with ${res.status}`);
  }
  return res.json() as Promise<DatasetsResponse>;
}

// GET /datasets/{dataset_id} - load a specific past dataset
export async function getDataset(dataset_id: string): Promise<UploadInfo> {
  const res = await fetch(`${API_BASE}/datasets/${encodeURIComponent(dataset_id)}`);
  if (!res.ok) {
    const msg = await res.text().catch(() => "");
    throw new Error(msg || `Failed to load dataset with ${res.status}`);
  }
  return res.json() as Promise<UploadInfo>;
}