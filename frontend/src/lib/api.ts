export const API_BASE = import.meta.env.VITE_API_BASE ?? "http://localhost:8000";

export async function healthz() {
  const res = await fetch(`${API_BASE}/healthz`);
  if (!res.ok) throw new Error("healthz failed");
  return res.json();
}
