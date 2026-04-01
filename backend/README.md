# AI Data Analyst Agent — FastAPI Backend

A production-ready FastAPI backend that converts natural-language questions about CSV datasets into executable data analysis code using pandas and AI.

## 🎯 Features

- **CSV Upload**: POST `/upload` accepts CSV files and returns a `dataset_id`
- **Natural Language Query**: POST `/query` accepts questions and returns analysis results
- **AI Integration**: Structured prompts sent to LLM (Groq API) for code generation
- **Safe Code Execution**: Sandbox execution with security filters to prevent dangerous operations
- **Query History**: All queries and responses stored in PostgreSQL
- **Database**: SQLAlchemy ORM with Supabase PostgreSQL backend
- **Error Handling**: Comprehensive error handling with meaningful HTTP responses
- **CORS Enabled**: Ready for frontend integration
- **Clean Architecture**: Modular structure with services, routes, and utilities

## 📁 Project Structure

```
backend/
├── app/
│   ├── __init__.py
│   ├── main.py                 # FastAPI app setup, CORS, lifespan
│   ├── models.py               # SQLAlchemy ORM models (Dataset, History)
│   ├── routes/
│   │   ├── __init__.py
│   │   ├── upload.py           # POST /upload endpoint
│   │   └── query.py            # POST /query + GET /history endpoints
│   ├── services/
│   │   ├── __init__.py
│   │   ├── ai_service.py       # LLM integration (structure for Groq)
│   │   ├── data_service.py     # CSV operations & safe code execution
│   │   └── llm_service.py      # [Optional] Additional LLM utilities
│   └── utils/
│       ├── __init__.py
│       ├── config.py           # Settings from .env
│       └── database.py         # SQLAlchemy engine & session
├── uploaded_datasets/          # Temporary CSV storage
├── requirements.txt            # Python dependencies
├── .env.example                # Environment variable template
└── README.md                   # This file
```

## 🚀 Quick Start

### 1️⃣ Prerequisites

- Python 3.9+
- PostgreSQL (or Supabase account for managed PostgreSQL)
- Groq API key (optional, mocks available for testing)

### 2️⃣ Installation

```bash
# Clone the repository
cd backend

# Create virtual environment
python -m venv .venv

# Activate
# On Windows:
.venv\Scripts\activate
# On macOS/Linux:
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

### 3️⃣ Environment Setup

```bash
# Copy .env.example to .env
cp .env.example .env

# Edit .env with your actual values
# Required:
#  - URL_SUPABASE: PostgreSQL connection string
#  - GROQ_API_KEY: (optional, mocked if empty)
```

### 4️⃣ Database Setup

```bash
# Create PostgreSQL databases and tables
# The app automatically creates tables on startup via SQLAlchemy
# Ensure your URL_SUPABASE connection string is correct

# Run the server (see Step 5) — tables will be created automatically
```

### 5️⃣ Run the Server

```bash
# Start development server
uvicorn app.main:app --reload

# Server runs at http://localhost:8000
# API docs: http://localhost:8000/docs
```

## 📖 API Endpoints

### Health Check
```
GET /
```
Returns API status and documentation link.

### Upload Dataset
```
POST /upload
```
**Request:** Multipart form with `file` (CSV)

**Response (200):**
```json
{
  "dataset_id": "550e8400-e29b-41d4-a716-446655440000",
  "filename": "sales_data.csv",
  "row_count": 1000,
  "columns": ["date", "product", "quantity", "price"],
  "message": "Dataset uploaded successfully."
}
```

**Errors:**
- `400`: Not a CSV file
- `400`: Empty file

---

### Query Dataset
```
POST /query
```

**Request:**
```json
{
  "dataset_id": "550e8400-e29b-41d4-a716-446655440000",
  "question": "What is the total revenue by product?"
}
```

**Response (200):**
```json
{
  "dataset_id": "550e8400-e29b-41d4-a716-446655440000",
  "question": "What is the total revenue by product?",
  "result": [
    {"product": "Laptop", "total_revenue": 50000},
    {"product": "Phone", "total_revenue": 30000}
  ],
  "history_id": "660e8400-e29b-41d4-a716-446655440001"
}
```

**Errors:**
- `404`: Dataset not found
- `422`: Unsafe code detected
- `500`: Code execution failed

---

### Get Query History
```
GET /history/{dataset_id}
```

**Response (200):**
```json
[
  {
    "id": "660e8400-e29b-41d4-a716-446655440001",
    "question": "What is the total revenue?",
    "answer": "[{'product': 'Laptop', 'total_revenue': 50000}]",
    "created_at": "2025-12-01T14:30:00.000Z"
  }
]
```

**Errors:**
- `404`: Dataset not found

---

## 🔐 Security

1. **Code Filtering**: Dangerous patterns blocked (os, subprocess, exec, eval, etc.)
2. **Sandboxed Execution**: `exec()` runs only in a restricted namespace
3. **DataFrame Copy**: Original data not modified
4. **Input Validation**: CSV validation on upload
5. **SQL Injection Protection**: SQLAlchemy parameterized queries

⚠️ **Note**: This is a basic security layer. For production, consider:
- Restricted execution environments (e.g., containers, AppArmor)
- Rate limiting
- Authentication/authorization
- Audit logging

---

## 🤖 AI Integration (Groq)

### Current State (Mock)

By default, `ai_service.generate_response()` returns a mock response suitable for testing.

### Enable Groq (Production)

1. Get API key from https://console.groq.com/keys
2. Add to `.env`:
   ```
   GROQ_API_KEY=gsk_xxxxxxxxxxxxxxxxxxxxx
   ```

3. Uncomment the Groq code in `app/services/ai_service.py`:
   ```python
   # Example in ai_service.py docstring:
   import httpx
   
   headers = {
       "Authorization": f"Bearer {settings.GROQ_API_KEY}",
       "Content-Type": "application/json",
   }
   payload = {
       "model": "llama3-70b-8192",
       "messages": [
           {"role": "system", "content": _build_system_prompt(schema)},
           {"role": "user",   "content": question},
       ],
       "temperature": 0,
   }
   r = httpx.post("https://api.groq.com/openai/v1/chat/completions",
                  json=payload, headers=headers, timeout=30)
   r.raise_for_status()
   return r.json()["choices"][0]["message"]["content"]
   ```

---

## 📊 Database Schema

### `datasets` Table
```sql
CREATE TABLE datasets (
    id VARCHAR PRIMARY KEY,
    name VARCHAR NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### `history` Table
```sql
CREATE TABLE history (
    id VARCHAR PRIMARY KEY,
    dataset_id VARCHAR NOT NULL FOREIGN KEY REFERENCES datasets(id),
    question TEXT NOT NULL,
    answer TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

---

## 🧪 Testing

### Test Upload Endpoint
```bash
curl -X POST "http://localhost:8000/upload" \
  -F "file=@sample.csv"
```

### Test Query Endpoint
```bash
curl -X POST "http://localhost:8000/query" \
  -H "Content-Type: application/json" \
  -d '{
    "dataset_id": "YOUR_DATASET_ID",
    "question": "How many rows are in the dataset?"
  }'
```

### Interactive API Docs
Open http://localhost:8000/docs (Swagger UI) or http://localhost:8000/redoc (ReDoc)

---

## 📝 Code Comments & Documentation

All files include comprehensive docstrings and inline comments explaining:
- Module responsibilities
- Function parameters and return types
- Key workflow steps
- Security considerations
- Integration points (e.g., where to wire up Groq)

---

## 🛠️ File Descriptions

### `main.py`
- FastAPI app initialization
- Lifespan setup (startup/shutdown)
- CORS configuration
- Route registration
- Health check endpoint

### `models.py`
- SQLAlchemy ORM definitions
- Dataset model (store metadata)
- History model (store queries)
- Relationship definitions

### `routes/upload.py`
- `POST /upload` endpoint
- CSV file validation
- Dataset ID generation
- Database record insertion

### `routes/query.py`
- `POST /query` endpoint
- `GET /history/{dataset_id}` endpoint
- Query validation
- LLM integration pipeline
- History persistence

### `services/ai_service.py`
- `generate_response()` function
- LLM prompt structure
- Mock response (for testing)
- Groq integration template

### `services/data_service.py`
- `save_dataset()` — Store CSV to disk
- `load_dataset()` — Load CSV from disk
- `get_schema()` — Extract metadata
- `execute_query_code()` — Safely execute pandas code
- `_is_safe_code()` — Security filter

### `utils/config.py`
- Load environment variables from `.env`
- Settings class with defaults
- Configuration constants

### `utils/database.py`
- SQLAlchemy engine creation
- Session factory setup
- `get_db()` FastAPI dependency
- ORM Base class

---

## 🔄 Data Flow

```
1. Client uploads CSV  →  POST /upload
   ↓
2. Backend validates file  →  CSV validation
   ↓
3. Generate UUID  →  dataset_id
   ↓
4. Save to disk  →  uploaded_datasets/<dataset_id>.csv
   ↓
5. Store metadata  →  INSERT INTO datasets
   ↓
6. Return dataset_id to client
   ↓
7. Client asks question  →  POST /query
   ↓
8. Load DataFrame  →  pd.read_csv()
   ↓
9. Extract schema  →  columns, dtypes, samples
   ↓
10. Send to LLM (Groq)  →  generate_response()
    ↓
11. LLM returns code  →  Python/pandas expressions
    ↓
12. Validate code  →  Security filter
    ↓
13. Execute on DataFrame  →  exec(code)
    ↓
14. Extract result variable  →  Python value
    ↓
15. Convert to JSON  →  Serialize for response
    ↓
16. Save to database  →  INSERT INTO history
    ↓
17. Return to client
```

---

## ⚙️ Configuration

All configuration is loaded from `.env` (see `.env.example`):

| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| `URL_SUPABASE` | str | (required) | PostgreSQL connection string |
| `GROQ_API_KEY` | str | "" | Groq API key (optional) |
| `UPLOAD_DIR` | str | "uploaded_datasets" | CSV storage directory |
| `CORS_ORIGIN` | str | "*" | CORS allowed origins |

---

## 🐛 Troubleshooting

### **Import Errors**
Ensure all packages in `requirements.txt` are installed:
```bash
pip install -r requirements.txt
```

### **Database Connection Failed**
- Check `URL_SUPABASE` in `.env`
- Ensure PostgreSQL is running
- Verify network connectivity to Supabase

### **CSV Upload Fails**
- Verify file is valid CSV (comma-separated, UTF-8)
- Check `UPLOAD_DIR` permission
- Ensure disk space available

### **Code Execution Error**
- Check generated code for syntax errors (visible in error response)
- Ensure DataFrame has expected columns
- Verify column names in question match actual data

### **Groq API Error**
- Verify `GROQ_API_KEY` is valid
- Check API rate limits
- Ensure network connectivity to Groq

---

## 📦 Dependencies

See `requirements.txt`:
- **fastapi**: Web framework
- **uvicorn**: ASGI server
- **sqlalchemy**: ORM
- **psycopg2-binary**: PostgreSQL driver
- **pandas**: Data manipulation
- **pydantic**: Data validation
- **python-dotenv**: Environment variables
- **python-multipart**: File upload handling
- **httpx**: HTTP client (for Groq API)

---

## 🚢 Deployment

### Docker
```dockerfile
FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
```

### Environment Variables for Production
- Use strong PostgreSQL passwords
- Keep `GROQ_API_KEY` secret (use secrets manager)
- Set `CORS_ORIGIN` to specific frontend URL
- Monitor `UPLOAD_DIR` disk usage (consider cleanup)

---

## 📚 Additional Resources

- [FastAPI Docs](https://fastapi.tiangolo.com/)
- [SQLAlchemy Docs](https://docs.sqlalchemy.org/)
- [Pandas Docs](https://pandas.pydata.org/docs/)
- [Groq API Docs](https://console.groq.com/docs)
- [Supabase Docs](https://supabase.com/docs)

---

## 📄 License

See the main project README.

---

**Last Updated**: January 2025 | **Version**: 1.0.0
