# Pandas/SQL online environment with runnable editors
**Upload a dataset; visualize and mess around with it**
**generate Pandas/SQL practice questions; + grading + review**

- A lightweight full-stack app for drilling practical Pandas/SQL skills on any input dataset.
- Designed for interview prep roles with your custom datasets.
- To use, you upload a CSV/Parquet file, the backend generates questions deterministically, and the frontend runs custom quiz questions with optional server-side grading.
- An SQL/pandas Python Editor is built in.


---

## Features

- **Dataset upload**: CSV/Parquet upload with preview and metadata
- **Question generation**: deterministic templates
- **Quiz runner**: MCQ + short-answer, keyboard-friendly navigation
- **Review mode**: show solutions, then review what you missed
- **Editor**: runnable SQL/Python pandas editor

---

## Tech Stack

**Backend**
- FastAPI (Python)
- pandas + pyarrow (for parsing/storage)

**Frontend**
- React + Vite + TypeScript
- Tailwind CSS
- Typed API client + clean component structure

---

### Prerequisites
- Python 3.11+
- Node 18+

### 1) Clone
```bash
git clone https://github.com/Kimbo7870/pandsqai.git
cd pandsqai
```

### 2) Run everything (recommended)
If you have a root Makefile:
```bash
make setup
make dev
```

### 3) Or run backend + frontend separately

#### Backend
```bash
cd backend
poetry install
poetry run uvicorn app.api:app --reload
```

#### Frontend
```bash
cd frontend
npm install
npm run dev
```

---

## Notes
To format backend python automatically, run:
```bash
poetry poetry run black .
```
from /backend

---
## Todo
- style and layout
- easier UI navigation
- implement actual backend SQL server to run PSQL
- fix quiz generation tab, haven't worked on it for a while
- attach openai api to generate llm generated quiz questions
- attach openai api to give feedback
- implement better results page
- add authentication
