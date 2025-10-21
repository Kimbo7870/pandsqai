import { useEffect, useMemo, useState, useCallback } from "react";
import { getQuestions } from "../lib/api";
import type { QuestionsResponse, Question } from "../lib/types";

interface Props {
  dataset_id: string;
  limit?: number;
  seed?: number;
}

/** Shape we actually read from your Question payloads */
type QuizItemShape = {
  id: string;
  prompt: string;
  choices?: string[];   // present => MCQ; absent => short-answer
  answer: unknown;      // server-side ground truth
};


function isRecord(v: unknown): v is Record<string, unknown> {
  return typeof v === "object" && v !== null;
}
function isStringArray(v: unknown): v is string[] {
  return Array.isArray(v) && v.every((x) => typeof x === "string");
}
function isQuizItem(q: Question): q is Question & QuizItemShape {
  if (!isRecord(q)) return false;
  const idOk = typeof q["id"] === "string";
  const promptOk = typeof q["prompt"] === "string";
  const choicesOk = q["choices"] === undefined || isStringArray(q["choices"]);
  const answerOk = "answer" in q;
  return idOk && promptOk && choicesOk && answerOk;
}

function isTypingInInput(e: KeyboardEvent): boolean {
  const t = e.target as HTMLElement | null;
  if (!t) return false;
  const tag = t.tagName.toLowerCase();
  const editable = (t as HTMLElement).isContentEditable;
  return editable || tag === "input" || tag === "textarea" || tag === "select";
}

/** Persisted user answer per question (union for MC vs short-answer) */
type SavedAnswer =
  | { kind: "mc"; index: number }
  | { kind: "text"; value: string }
  | undefined;

/** Safely extract choices; null if absent -> short-answer */
function getChoices(q: Question): string[] | null {
  if (!isQuizItem(q)) return null;
  return q.choices ?? null;
}

/** Normalize any value to a string for comparison */
function toAnswerString(ans: unknown): string {
  if (ans === null || ans === undefined) return "";
  if (typeof ans === "string" || typeof ans === "number" || typeof ans === "boolean") {
    return String(ans);
  }
  try {
    return JSON.stringify(ans);
  } catch {
    return String(ans);
  }
}

/** Light normalization for user-entered text */
function normalizeText(s: string): string {
  return s.trim().replace(/\s+/g, " ").toLowerCase();
}

/** For MCQ: find index of correct choice by string equality */
function getCorrectIndex(q: Question, choices: string[] | null): number | null {
  if (!choices || !isQuizItem(q)) return null;
  const ansStr = toAnswerString(q.answer);
  const idx = choices.findIndex((c) => String(c) === ansStr);
  return idx >= 0 ? idx : null;
}

/** For short-answer: compare normalized strings */
function isShortAnswerCorrect(q: Question, userText: string): boolean {
  if (!isQuizItem(q)) return false;
  const truth = normalizeText(toAnswerString(q.answer));
  const got = normalizeText(userText);
  return truth.length > 0 && got === truth;
}

/** Recompute score from saved answers against the current questions */
function computeScore(
  answers: Record<string, SavedAnswer>,
  questions: Question[]
): number {
  let s = 0;
  for (const q of questions) {
    if (!isQuizItem(q)) continue;
    const saved = answers[q.id];
    const choices = getChoices(q);
    if (choices) {
      const correctIdx = getCorrectIndex(q, choices);
      if (saved?.kind === "mc" && correctIdx != null && saved.index === correctIdx) {
        s += 1;
      }
    } else {
      if (saved?.kind === "text" && isShortAnswerCorrect(q, saved.value)) {
        s += 1;
      }
    }
  }
  return s;
}

export default function QuizPlay({ dataset_id, limit = 8, seed = 0 }: Props) {
  const [data, setData] = useState<QuestionsResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [err, setErr] = useState("");

  const [i, setI] = useState(0);                       // current question index
  const [locked, setLocked] = useState(false);         // after submit
  const [score, setScore] = useState(0);
  const [answerMap, setAnswerMap] = useState<Record<string, SavedAnswer>>({});

  // Per-question working state (not persisted until Submit)
  const [selectedIndex, setSelectedIndex] = useState<number | null>(null); // MCQ
  const [textInput, setTextInput] = useState<string>("");                  // Short-answer

  const q: Question | undefined = data?.questions?.[i];
  const choices = useMemo(() => (q ? getChoices(q) : null), [q]);

  // Derived correctness references
  const correctIndex = useMemo(
    () => (q ? getCorrectIndex(q, choices) : null),
    [q, choices]
  );
  const qid: string | null = q && isQuizItem(q) ? q.id : null;

    const onSubmit = useCallback(() => {
    if (!q || !isQuizItem(q) || !data || !qid) return;
    if (locked) return;

    const isMC = !!choices;

    if (isMC) {
        if (selectedIndex === null) return; // ensure number

        setAnswerMap((prev): Record<string, SavedAnswer> => {
        const next: Record<string, SavedAnswer> = { ...prev };
        const saved: SavedAnswer = { kind: "mc", index: selectedIndex };
        next[qid] = saved;
        setScore(computeScore(next, data.questions));
        return next;
        });

        setLocked(true);
        return;
    }

    // short-answer branch
    setAnswerMap((prev): Record<string, SavedAnswer> => {
        const next: Record<string, SavedAnswer> = { ...prev };
        const saved: SavedAnswer = { kind: "text", value: textInput };
        next[qid] = saved;
        setScore(computeScore(next, data.questions));
        return next;
    });

    setLocked(true);
  }, [q, data, qid, locked, choices, selectedIndex, textInput]);

  useEffect(() => {
  function onKey(e: KeyboardEvent) {
    // allow Enter to submit even when focused in an input/textarea
    if (isTypingInInput(e) && e.key !== "Enter") return;
    if (!q || !isQuizItem(q)) return;

    // Left/Right navigation
    if (e.key === "ArrowLeft") {
      e.preventDefault();
      if (i > 0) setI(i - 1);
      return;
    }
    if (e.key === "ArrowRight") {
      e.preventDefault();
      if (data && i < data.questions.length - 1) setI(i + 1);
      return;
    }

    // Enter submits (only if allowed)
    if (e.key === "Enter") {
      const hasAnswer = choices ? selectedIndex !== null : textInput.trim().length > 0;
      if (!locked && hasAnswer) {
        e.preventDefault();
        onSubmit();
      }
      return;
    }

    // Number keys 1..9 → pick that choice (MC only, not locked)
    if (choices && !locked) {
      const n = parseInt(e.key, 10);
      if (!Number.isNaN(n) && n >= 1 && n <= Math.min(9, choices.length)) {
        e.preventDefault();
        setSelectedIndex(n - 1);
      }
    }
  }

  window.addEventListener("keydown", onKey);
  return () => window.removeEventListener("keydown", onKey);
}, [q, i, data, choices, locked, selectedIndex, textInput, onSubmit]);

  // Initial fetch
  useEffect(() => {
    async function run() {
      setLoading(true);
      setErr("");
      try {
        const res = await getQuestions(dataset_id, limit, seed);
        setData(res);
        setI(0);

        // Initialize first question UI from saved answers, if any
        const first = res.questions[0];
        if (first && isQuizItem(first)) {
          const saved = answerMap[first.id];
          if (saved?.kind === "mc") {
            setSelectedIndex(saved.index);
            setTextInput("");
            setLocked(true);
          } else if (saved?.kind === "text") {
            setTextInput(saved.value);
            setSelectedIndex(null);
            setLocked(true);
          } else {
            setSelectedIndex(null);
            setTextInput("");
            setLocked(false);
          }
          setScore(computeScore(answerMap, res.questions));
        } else {
          setSelectedIndex(null);
          setTextInput("");
          setLocked(false);
          setScore(0);
        }
      } catch (e: unknown) {
        setErr(e instanceof Error ? e.message : String(e));
      } finally {
        setLoading(false);
      }
    }
    run();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [dataset_id, limit, seed]);

  // When the question changes, restore its saved state
  useEffect(() => {
    if (!q || !isQuizItem(q)) return;
    const saved = answerMap[q.id];
    if (saved?.kind === "mc") {
      setSelectedIndex(saved.index);
      setTextInput("");
      setLocked(true);
    } else if (saved?.kind === "text") {
      setSelectedIndex(null);
      setTextInput(saved.value);
      setLocked(true);
    } else {
      setSelectedIndex(null);
      setTextInput("");
      setLocked(false);
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [q]);

  if (loading) return <p className="text-gray-600">Loading quiz…</p>;
  if (err) return <p className="text-red-600">{err}</p>;
  if (!data || data.questions.length === 0) return <p>No questions.</p>;

  const total = data.questions.length;
  const stem =
    q && isQuizItem(q) ? q.prompt : "Answer the following question:";

  function go(delta: number) {
    const next = Math.min(total - 1, Math.max(0, i + delta));
    setI(next);
  }

  // Feedback helpers
  const isMC = !!choices;
  const mcCorrect =
    isMC && correctIndex != null && selectedIndex != null
      ? selectedIndex === correctIndex
      : false;
  const textCorrect =
    !isMC && q ? isShortAnswerCorrect(q, textInput) : false;

  return (
    <div className="space-y-4">
      {/* Header / Progress */}
      <div className="flex items-center justify-between">
        <div className="text-sm text-gray-700">
          <b>Question:</b> {i + 1}/{total} • <b>Score:</b> {score}
        </div>
        <div className="text-xs text-gray-500">
          <span className="font-mono">seed={seed}</span>
        </div>
      </div>

      {/* Stem */}
      <div className="p-4 border rounded">
        <p className="text-sm">{stem}</p>
      </div>

      {/* MCQ OR Short-answer */}
      {isMC ? (
        <fieldset className="space-y-2">
          {choices!.map((c, idx) => {
            const selectedCls =
              selectedIndex === idx
                ? "border-blue-600 ring-1 ring-blue-600"
                : "border-gray-200";
            const feedback =
              locked && idx === correctIndex
                ? "bg-green-50 border-green-500"
                : locked && selectedIndex === idx && idx !== correctIndex
                ? "bg-red-50 border-red-500"
                : "";
            const groupName = `q-${qid ?? `q-${i}`}`;
            return (
              <label
                key={`${groupName}-choice-${idx}`}
                className={`flex items-start gap-3 p-3 border rounded cursor-pointer ${selectedCls} ${feedback}`}
              >
                <input
                  type="radio"
                  name={groupName}
                  className="mt-1"
                  disabled={locked}
                  checked={selectedIndex === idx}
                  onChange={() => setSelectedIndex(idx)}
                />
                <span className="text-sm">{String(c)}</span>
              </label>
            );
          })}
        </fieldset>
      ) : (
        <div className="space-y-2">
          <label className="block text-sm text-gray-700">Your answer</label>
          <input
            type="text"
            value={textInput}
            onChange={(e) => setTextInput(e.target.value)}
            disabled={locked}
            className="w-full px-3 py-2 border rounded text-sm outline-none focus:ring-2 focus:ring-blue-600"
            placeholder="Type your answer…"
          />
          {locked && (
            <div className="text-sm">
              {textCorrect ? (
                <span className="text-green-700">Correct</span>
              ) : (
                <span className="text-red-700">
                  Incorrect — correct answer:{" "}
                  <span className="font-mono">
                    {q ? toAnswerString((q as QuizItemShape).answer) : ""}
                  </span>
                </span>
              )}
            </div>
          )}
        </div>
      )}

      {/* Actions */}
      <div className="flex items-center gap-3">
        <button
          className="px-3 py-2 rounded bg-gray-100"
          onClick={() => go(-1)}
          disabled={i === 0}
        >
          Prev
        </button>
        <button
          className="px-3 py-2 rounded bg-blue-600 text-white"
          onClick={onSubmit}
          disabled={
            locked ||
            (isMC ? selectedIndex == null : textInput.trim().length === 0)
          }
        >
          Submit
        </button>
        <button
          className="px-3 py-2 rounded bg-gray-100"
          onClick={() => go(+1)}
          disabled={i === total - 1}
        >
          Next
        </button>

        {/* Unified feedback tag for MC or text */}
        {locked && (
          <span className="text-sm">
            {(isMC && mcCorrect) || (!isMC && textCorrect) ? (
              <span className="text-green-700">Correct</span>
            ) : (
              <span className="text-red-700">Incorrect</span>
            )}
          </span>
        )}
      </div>
    </div>
  );
}
