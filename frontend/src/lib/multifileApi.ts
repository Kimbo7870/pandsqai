import { API_BASE } from "./api";
import type {
  MultiCurrentResponse,
  MultiDeleteResponse,
  MultiUploadResponse,
} from "./multifileTypes";

export async function getMultiCurrent(): Promise<MultiCurrentResponse> {
  const res = await fetch(`${API_BASE}/multifile/current`);
  if (!res.ok) {
    const msg = await res.text().catch(() => "");
    throw new Error(msg || `Failed to load multifile current set (${res.status})`);
  }
  return res.json() as Promise<MultiCurrentResponse>;
}

export async function uploadMultiDataset(file: File): Promise<MultiUploadResponse> {
  const fd = new FormData();
  fd.append("file", file);
  const res = await fetch(`${API_BASE}/multifile/upload`, {
    method: "POST",
    body: fd,
  });
  if (!res.ok) {
    const msg = await res.text().catch(() => "");
    throw new Error(msg || `Multifile upload failed (${res.status})`);
  }
  return res.json() as Promise<MultiUploadResponse>;
}

export async function deleteMultiDataset(dataset_id: string): Promise<MultiDeleteResponse> {
  const res = await fetch(`${API_BASE}/multifile/current/${encodeURIComponent(dataset_id)}`, {
    method: "DELETE",
  });
  if (!res.ok) {
    const msg = await res.text().catch(() => "");
    throw new Error(msg || `Failed to delete dataset (${res.status})`);
  }
  return res.json() as Promise<MultiDeleteResponse>;
}

export async function deleteAllMultiDatasets(): Promise<MultiDeleteResponse> {
  const res = await fetch(`${API_BASE}/multifile/current`, { method: "DELETE" });
  if (!res.ok) {
    const msg = await res.text().catch(() => "");
    throw new Error(msg || `Failed to delete all datasets (${res.status})`);
  }
  return res.json() as Promise<MultiDeleteResponse>;
}
