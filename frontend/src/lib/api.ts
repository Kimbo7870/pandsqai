// API helper functions to connect to FastAPI backend, will have fetch logic

import type { UploadInfo, ProfileInfo } from "./types";

export const API_BASE = import.meta.env.VITE_API_BASE ?? "http://localhost:8000";


// sends a CSV or Parquet (multipart/form-data) to POST /upload
// multipart/form-data is HTTP content type for HTML forms that include files
export async function uploadDataset(file: File): Promise<UploadInfo> {
  const fd = new FormData(); // crate multipart/form-data
  fd.append("file", file); // key name file must match FastAPI param
  const res = await fetch(`${API_BASE}/upload`, { method: "POST", body: fd }); // POST request

  if (!res.ok) {
    const msg = await res.text().catch(() => "");
    throw new Error(msg || `Upload failed with ${res.status}`);
  }

  return res.json() as Promise<UploadInfo>; // Type assertion is dev-time, care, trusts backend matches UploadInfo.
}

export async function getProfile(dataset_id: string): Promise<ProfileInfo> {
  const res = await fetch(`${API_BASE}/profile?dataset_id=${dataset_id}`);
  
  if (!res.ok) {
    const msg = await res.text().catch(() => "");
    throw new Error(msg || `Profile failed with ${res.status}`);
  }

  return res.json() as Promise<ProfileInfo>;
}