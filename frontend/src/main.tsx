import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { BrowserRouter, Route, Routes } from "react-router-dom";
import "./index.css";
import App from "./App.tsx";
import NotFound from "./components/NotFound";
import MultiFileHome from "./multifile/MultiFileHome";
import MultiFileViewAll from "./multifile/MultiFileViewAll";

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <BrowserRouter>
      <Routes>
        <Route path="/" element={<App />} />
        <Route path="/multifile" element={<MultiFileHome />} />
        <Route path="/multifile/view-all" element={<MultiFileViewAll />} />
        <Route path="*" element={<NotFound />} />
      </Routes>
    </BrowserRouter>
  </StrictMode>,
);
