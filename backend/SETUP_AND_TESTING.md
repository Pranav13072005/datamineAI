# Setup & Testing Guide

Complete step-by-step guide to set up and test the AI Data Analyst Agent backend.

## Table of Contents

1. [Environment Setup](#environment-setup)
2. [Database Configuration](#database-configuration)
3. [Local Development](#local-development)
4. [Testing the API](#testing-the-api)
5. [Troubleshooting](#troubleshooting)

---

## Environment Setup

### Step 1: Create Virtual Environment

```bash
cd backend

# Windows
python -m venv .venv
.venv\Scripts\activate

# macOS / Linux
python3 -m venv .venv
source .venv/bin/activate
```

### Step 2: Install Dependencies

```bash
pip install -r requirements.txt
```

### Step 3: Configure Environment Variables

```bash
# Copy the template
cp .env.example .env

# Edit .env with your actual values (see below)
```

---

## Database Configuration

### Option A: Using Supabase (Recommended for Cloud)

1. **Create Supabase Account**
   - Go to https://supabase.com
   - Sign up for free
   - Create a new project

2. **Get Connection String**
   - In Supabase dashboard → Project Settings → Database
   - Copy the "PostgreSQL" connection string
   - Format: `postgresql://postgres:[PASSWORD]@[HOST]:[PORT]/postgres`

3. **Update .env**
   ```
   URL_SUPABASE=postgresql://postgres:your_password@db.xxxxx.supabase.co:5432/postgres
   ```

### Option B: Using Local PostgreSQL

1. **Install PostgreSQL**
   ```bash
   # Windows: Download from https://www.postgresql.org/download/windows/
   # macOS: brew install postgresql
   # Linux: sudo apt-get install postgresql
   ```

2. **Start PostgreSQL Service**
   ```bash
   # Windows: Services app → PostgreSQL → Start
   # macOS: brew services start postgresql
   # Linux: sudo service postgresql start
   ```

3. **Create Database & User**
   ```bash
   psql -U postgres
   
   CREATE DATABASE ai_analyst;
   CREATE USER analyst WITH PASSWORD 'your_secure_password';
   GRANT ALL PRIVILEGES ON DATABASE ai_analyst TO analyst;
   ```

4. **Update .env**
   ```
   URL_SUPABASE=postgresql://analyst:your_secure_password@localhost:5432/ai_analyst
   ```

### Step 4: Verify Connection

The app tests the database on startup. Run the server and check the output:

```bash
uvicorn app.main:app --reload
```

You should see:
```
===========================================
🚀  AI Data Analyst API — starting up...
📦  Database tables verified / created.
✅  Successfully connected to Supabase Database!
===========================================
```

---

## Local Development

### Start the Server

```bash
# Make sure .venv is activated
cd backend

# Run with auto-reload (development)
uvicorn app.main:app --reload

# Output should show:
# INFO:     Uvicorn running on http://127.0.0.1:8000
# INFO:     Application startup complete
```

### Open API Documentation

- **Swagger UI**: http://localhost:8000/docs
- **ReDoc**: http://localhost:8000/redoc

Try the endpoints interactively with these tools.

---

## Testing the API

### Test 1: Health Check

```bash
curl http://localhost:8000/
```

Expected response:
```json
{
  "status": "ok",
  "message": "Welcome to the AI Data Analyst API",
  "docs": "/docs"
}
```

### Test 2: Upload a CSV File

Create a sample CSV file (`sample_sales.csv`):
```csv
date,product,quantity,price,revenue
2025-01-01,Laptop,2,1000,2000
2025-01-01,Phone,5,500,2500
2025-01-02,Laptop,1,1000,1000
2025-01-02,Tablet,3,300,900
2025-01-03,Phone,8,500,4000
```

Upload it:
```bash
curl -X POST "http://localhost:8000/upload" \
  -F "file=@sample_sales.csv"
```

Expected response (save `DATASET_ID` for next tests):
```json
{
  "dataset_id": "550e8400-e29b-41d4-a716-446655440000",
  "filename": "sample_sales.csv",
  "row_count": 5,
  "columns": ["date", "product", "quantity", "price", "revenue"],
  "message": "Dataset uploaded successfully."
}
```

### Test 3: Query the Dataset

Replace `YOUR_DATASET_ID` with the `dataset_id` from Test 2:

```bash
curl -X POST "http://localhost:8000/query" \
  -H "Content-Type: application/json" \
  -d '{
    "dataset_id": "YOUR_DATASET_ID",
    "question": "What is the total revenue?"
  }'
```

Expected response:
```json
{
  "dataset_id": "YOUR_DATASET_ID",
  "question": "What is the total revenue?",
  "result": "Dataset has 5 rows with columns: ['date', 'product', 'quantity', 'price', 'revenue']. (AI integration pending — wire up Groq in ai_service.py)",
  "history_id": "660e8400-e29b-41d4-a716-446655440001"
}
```

⚠️ **Note**: This returns a mock response because Groq is not configured. To use real AI, set `GROQ_API_KEY` in `.env`.

### Test 4: Retrieve Query History

```bash
curl "http://localhost:8000/history/YOUR_DATASET_ID"
```

Expected response:
```json
[
  {
    "id": "660e8400-e29b-41d4-a716-446655440001",
    "question": "What is the total revenue?",
    "answer": "Dataset has 5 rows with columns: [...]. (AI integration pending...)",
    "created_at": "2025-01-15T10:30:00"
  }
]
```

### Test 5: Test Error Handling

**Missing dataset:**
```bash
curl -X POST "http://localhost:8000/query" \
  -H "Content-Type: application/json" \
  -d '{
    "dataset_id": "nonexistent-id",
    "question": "test"
  }'
```

Expected response (404):
```json
{
  "detail": "Dataset 'nonexistent-id' not found. Upload it first via POST /upload."
}
```

**Invalid CSV file:**
```bash
echo "not a csv" > invalid.txt
curl -X POST "http://localhost:8000/upload" \
  -F "file=@invalid.txt"
```

Expected response (400):
```json
{
  "detail": "Only .csv files are accepted."
}
```

---

## Advanced Testing

### Enable Groq AI Integration

1. **Get Groq API Key**
   - Visit https://console.groq.com/keys
   - Create a new API key

2. **Update .env**
   ```
   GROQ_API_KEY=gsk_your_actual_key_here
   ```

3. **Uncomment Groq Code in `ai_service.py`**
   - See the function `generate_response()`
   - Replace the mock response section with the commented Groq example

4. **Restart Server**
   ```bash
   # Press Ctrl+C to stop
   # Run again:
   uvicorn app.main:app --reload
   ```

5. **Test Again**
   ```bash
   curl -X POST "http://localhost:8000/query" \
     -H "Content-Type: application/json" \
     -d '{
       "dataset_id": "YOUR_DATASET_ID",
       "question": "Calculate the average quantity per product"
     }'
   ```

   Now you'll get actual AI-generated code execution!

### Test with Complex Queries

Create more complex CSV with multiple columns and data types:

```csv
id,date,customer,product,quantity,price,discount,region
1,2025-01-01,Alice,Laptop,2,1000,0.1,US
2,2025-01-01,Bob,Phone,5,500,0.0,EU
3,2025-01-02,Alice,Tablet,1,300,0.2,US
4,2025-01-02,Charlie,Laptop,1,1000,0.15,Asia
5,2025-01-03,Bob,Phone,3,500,0.05,EU
```

Try queries like:
- "How many sales per region?"
- "What is the average discount?"
- "Which customer has the highest total purchase value?"
- "Show top 3 products by quantity sold"

---

## Database Inspection

### View Tables in Supabase

```bash
# Using psql locally:
psql -U analyst -d ai_analyst

# Then:
\dt                    # List all tables
\d datasets            # Show datasets table structure
\d history             # Show history table structure
SELECT * FROM datasets;
SELECT * FROM history;
\q                     # Quit
```

### Monitor Query History

```bash
SELECT h.question, h.answer, h.created_at 
FROM history h 
JOIN datasets d ON h.dataset_id = d.id
ORDER BY h.created_at DESC
LIMIT 10;
```

---

## Debugging

### Enable SQL Logging

Edit `app/utils/database.py`:
```python
engine = create_engine(
    settings.URL_SUPABASE,
    pool_pre_ping=True,
    echo=True,  # ← Change False to True
)
```

This shows all SQL queries in the terminal.

### Check Generated Code

Add print statements in `app/services/ai_service.py`:
```python
def generate_response(question: str, schema: dict) -> str:
    # ... existing code ...
    mock_code = (...)
    print(f"Generated code:\n{mock_code}")  # ← Add this
    return mock_code
```

### Monitor Code Execution

Add print statements in `app/services/data_service.py`:
```python
def execute_query_code(code: str, df: pd.DataFrame) -> Any:
    # ... existing code ...
    try:
        exec(code, exec_globals)
        print(f"Execution successful. Result: {exec_globals.get('result')}")
    except Exception:
        # ...
```

---

## Troubleshooting

### Error: `ModuleNotFoundError: No module named 'fastapi'`

**Solution:**
```bash
# Ensure venv is activated
pip install -r requirements.txt
```

### Error: `could not connect to server`

**Solution:**
- Check PostgreSQL is running
- Verify `URL_SUPABASE` in `.env`
- Test connection manually:
  ```bash
  psql -h localhost -U analyst -d ai_analyst
  ```

### Error: `Table 'datasets' already exists`

**Solution:** This is normal. SQLAlchemy checks for tables and creates them if they don't exist. Safe to ignore.

### Error: `CORS error from frontend`

**Solution:** Update `.env`:
```
CORS_ORIGIN=http://localhost:3000
# or for development:
CORS_ORIGIN=*
```

Then restart the server.

### CSV Upload Fails

**Solutions:**
1. Verify file is valid CSV (comma-separated)
2. Check file encoding is UTF-8
3. Ensure `UPLOAD_DIR` exists and is writable
4. Check disk space

### "Unsafe code blocked" Error

The security filter rejected the generated code. This is working as intended. The LLM tried to use forbidden functions like `os.remove()` or `eval()`.

---

## Performance Optimization

### For Large Datasets

1. **Chunk CSV Reading** (modify `data_service.py`):
   ```python
   # Instead of reading entire file:
   df = pd.read_csv(path)
   
   # For large files, limit rows:
   df = pd.read_csv(path, nrows=50000)
   ```

2. **Add Pagination to History**:
   ```python
   # In query.py, add limit/offset parameters
   ```

3. **Cache DataFrames** (if repeatedly queried):
   ```python
   # Simple in-memory cache
   _cache = {}
   
   def load_dataset_cached(dataset_id):
       if dataset_id not in _cache:
           _cache[dataset_id] = pd.read_csv(...)
       return _cache[dataset_id]
   ```

---

## Production Checklist

- [ ] Set `GROQ_API_KEY` properly
- [ ] Use strong database password
- [ ] Set specific `CORS_ORIGIN` (not `*`)
- [ ] Enable HTTPS
- [ ] Add rate limiting
- [ ] Enable authentication
- [ ] Monitor disk usage for `UPLOAD_DIR`
- [ ] Set up logging
- [ ] Regular database backups
- [ ] Use gunicorn instead of uvicorn:
  ```bash
  pip install gunicorn
  gunicorn app.main:app -w 4 -k uvicorn.workers.UvicornWorker
  ```

---

**Now you're ready to use the AI Data Analyst Agent!** 🚀

For more details, see [README.md](README.md).
