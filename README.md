An AI-powered data analysis platform that profiles datasets, detects anomalies, forecasts trends, and answers natural language questions — automatically, before you even ask.

---

## What it does

Upload a CSV or Excel file. Within seconds the system surfaces correlations, flags anomalies, identifies data quality issues, and proposes fixes. Ask questions in plain English and get back structured answers with tables, charts, and insights. Query across multiple datasets using semantic search — even when column names don't match.

---

## Tech stack

| Layer | Technology |
| --- | --- |
| Backend | FastAPI, SQLAlchemy, Alembic |
| Frontend | React, Vite, react-plotly.js |
| Database | PostgreSQL (Supabase), pgvector |
| LLM | Groq (Llama 3.3 70B) |
| ML models | scikit-learn, statsmodels |
| Embeddings | sentence-transformers (MiniLM-L6) |
| Storage | Supabase Storage |
| Background jobs | FastAPI BackgroundTasks → Celery + Redis |
| Auth | JWT (python-jose) |
| Containerisation | Docker Compose |

---

## Project structure

```
backend/
  app/
    main.py
    config.py
    routers/
      datasets.py
      query.py
      export.py
      health.py
      auth.py
    services/
      dataset_service.py
      query_classifier.py
      descriptive_handler.py
      analytical_handler.py
      ml_models.py
      ml_handler.py
      insight_extractor.py
      missing_value_handler.py
      embedding_service.py
      history_service.py
      agent_planner.py
      code_executor.py
      storage_service.py
    models/
      dataset.py
      query_history.py
    schemas/
      query.py
      dataset.py
    middleware/
      logging_middleware.py
  tests/
  alembic/
  requirements.txt
  Dockerfile

frontend/
  src/
    components/
      QueryResult.jsx
      DataReportCard.jsx
      MissingValueEditor.jsx
      CleaningProposal.jsx
      HistoryPanel.jsx
  Dockerfile

docker-compose.yml
.env.example
```

---

## Getting started

### Prerequisites

- Docker and Docker Compose
- A Groq API key (free at [console.groq.com](http://console.groq.com))
- A Supabase project (free tier works)

### 1. Clone and configure

```bash
git clone https://github.com/yourname/datamineai
cd datamineai
cp .env.example .env
```

Open `.env` and fill in:

```
GROQ_API_KEY=your_groq_key
DATABASE_URL=postgresql+asyncpg://user:pass@localhost:5432/datamineai
SUPABASE_URL=https://yourproject.supabase.co
SUPABASE_KEY=your_supabase_anon_key
SECRET_KEY=any_long_random_string
REDIS_URL=redis://redis:6379/0
MAX_UPLOAD_MB=100
USE_LOCAL_STORAGE=false
```

### 2. Start everything

```bash
docker compose up
```

This starts PostgreSQL with pgvector, Redis, the FastAPI backend, a Celery worker, and the React frontend.

### 3. Run migrations

```bash
docker compose exec backend alembic upgrade head
```

### 4. Open the app

Frontend: http://localhost:5173

API docs: http://localhost:8000/docs

Health check: http://localhost:8000/health

---

## How the query pipeline works

Every natural language question is routed through a classifier before any LLM is called:

```
User question
      │
      ▼
 classify_query()
      │
      ├── "descriptive"  →  deterministic Pandas handler (no LLM)
      ├── "smalltalk"    →  static response
      ├── "anomaly"      →  pre-computed IsolationForest result
      ├── "clustering"   →  pre-computed KMeans result
      ├── "forecast"     →  pre-computed ARIMA result
      └── "analytical"   →  two-stage LLM agent
                                  │
                                  ├── Planner: decomposes into sub-tasks
                                  ├── Executor: runs each tool
                                  └── Synthesiser: narrates combined result
```

ML results (anomalies, clusters, forecasts) are computed as background jobs on upload and served instantly — no LLM call at query time.

Add `?mode=fast` to any `/query` request to bypass the planner and use the direct single-shot LLM path.

---

## API reference

### Health

```
GET  /health
```

### Auth

```
POST /auth/register       {email, password}
POST /auth/login          {email, password}
```

### Datasets

```
POST   /datasets/upload
GET    /datasets
GET    /datasets/{id}
GET    /datasets/{id}/schema
GET    /datasets/{id}/insights
GET    /datasets/{id}/missing-analysis
GET    /datasets/{id}/cleaning-plan
GET    /datasets/{id}/history
GET    /datasets/{id}/download
GET    /datasets/{id}/download?format=xlsx
DELETE /datasets/{id}
GET    /datasets/search?q={query}

POST   /datasets/{id}/apply-cleaning       {issue_ids}
POST   /datasets/{id}/missing-value-preview {fixes}
POST   /datasets/{id}/apply-missing-fixes   {fixes}
```

### Query

```
POST /query    {dataset_id, question}
```

Response shape:

```json
{
  "answer": "string",
  "table": { "columns": [], "rows": [] },
  "chart": { "type": "bar", "data": {} },
  "insights": ["string"],
  "warnings": ["string"],
  "query_type": "analytical",
  "related_history": []
}
```

### Export

```
POST /export/pdf    {dataset_id, query_response}
```

---

## Environment variables

| Variable | Required | Default | Description |
| --- | --- | --- | --- |
| `GROQ_API_KEY` | Yes | — | Groq API key |
| `DATABASE_URL` | Yes | — | Postgres connection string |
| `SUPABASE_URL` | Yes | — | Supabase project URL |
| `SUPABASE_KEY` | Yes | — | Supabase anon key |
| `SECRET_KEY` | Yes | — | JWT signing secret |
| `REDIS_URL` | Yes | — | Redis connection string |
| `MAX_UPLOAD_MB` | No | 50 | Max upload size in MB |
| `USE_LOCAL_STORAGE` | No | false | Use local disk instead of Supabase Storage |
| `QUERY_RATE_LIMIT` | No | 20/minute | Queries per user per minute |
| `UPLOAD_RATE_LIMIT` | No | 5/minute | Uploads per user per minute |
| `EMBEDDING_MODEL` | No | all-MiniLM-L6-v2 | Sentence transformer model |

---

## Running tests

```bash
docker compose exec backend pytest tests/ -v
```

The test suite uses an in-memory SQLite database and httpx async client. No external services required to run tests.

---

## Safety model

User-submitted pandas code is executed in a restricted namespace:

- Blocked imports: `os`, `sys`, `subprocess`, `requests`, `socket`, `http`
- Blocked builtins: `open`, `exec`, `eval`, `__import__`
- Execution timeout: 10 seconds via `ThreadPoolExecutor`
- Namespace contains only: `df` (the dataset), `pd` (pandas)
- All results are JSON-sanitised before returning (no numpy types leak)

---

## Rate limits

- `/query` — 20 requests per user per minute
- `/datasets/upload` — 5 requests per user per minute

Both limits are configurable via environment variables. Exceeding a limit returns `429` with a `retry_after` field.

---

## Known limitations

- Supported file types: CSV, Excel (.xlsx, .xls). PDF and Word document ingestion is not yet implemented.
- ARIMA forecasting requires a detectable datetime column and at least 30 data points.
- The embedding model (`all-MiniLM-L6-v2`) loads on backend startup — first start takes ~15 seconds while the model downloads.
- Clustering is skipped on datasets with fewer than 50 rows or fewer than 3 numeric columns.

---

## Roadmap

- [ ]  PDF and Word document ingestion via document loaders
- [ ]  Google Sheets connector
- [ ]  Scheduled analysis reports (daily/weekly email digest)
- [ ]  Collaborative workspaces (share datasets across users)
- [ ]  Fine-tuned prompt templates per industry vertical (finance, e-commerce, healthcare)

---

## License

MIT
