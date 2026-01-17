import { useEffect } from "react";

type Props = {
  open: boolean;
  title: string;
  subtitle?: string;
  onClose: () => void;
  children: React.ReactNode;
};

export default function EditorDrawer({ open, title, subtitle, onClose, children }: Props) {
  // Close on Escape
  useEffect(() => {
    if (!open) return;
    function onKeyDown(e: KeyboardEvent) {
      if (e.key === "Escape") onClose();
    }
    window.addEventListener("keydown", onKeyDown);
    return () => window.removeEventListener("keydown", onKeyDown);
  }, [open, onClose]);

  return (
    <>
      {/* backdrop */}
      <div
        className={
          "fixed inset-0 bg-black/30 transition-opacity " +
          (open ? "opacity-100 pointer-events-auto" : "opacity-0 pointer-events-none")
        }
        onClick={onClose}
      />

      {/* drawer */}
      <div
        className={
          "fixed top-0 right-0 h-full w-[min(50vw,760px)] max-w-[95vw] bg-white border-l border-gray-300 shadow-xl transition-transform duration-200 " +
          (open ? "translate-x-0" : "translate-x-full")
        }
        aria-hidden={!open}
      >
        <div className="h-full flex flex-col">
          <div className="p-4 border-b border-gray-200 flex items-start justify-between gap-3">
            <div className="min-w-0">
              <div className="text-base font-semibold truncate">{title}</div>
              {subtitle && <div className="text-sm text-gray-600 truncate">{subtitle}</div>}
            </div>
            <button
              onClick={onClose}
              className="px-3 py-1.5 bg-gray-600 text-white rounded hover:bg-gray-700 active:bg-gray-800 transition-colors"
            >
              Close
            </button>
          </div>

          <div className="flex-1 overflow-auto p-4">{children}</div>
        </div>
      </div>
    </>
  );
}
