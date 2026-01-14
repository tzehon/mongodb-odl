# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

MongoDB Atlas Operational Data Layer (ODL) demo that streams banking data from Databricks Lakehouse (Delta Lake) to MongoDB Atlas, with a FastAPI backend serving a React dashboard.

**Target SLAs:** 500 QPS, <100ms p95 latency

## Common Commands

### Run the Full Stack
```bash
# Copy and configure environment
cp .env.example .env
# Edit .env with your MONGODB_URI

# Start all services (API, Frontend, Locust)
docker-compose up --build
```

**Services:**
- Dashboard: http://localhost:3000
- API Docs: http://localhost:8000/docs
- Load Testing: http://localhost:8089

### Development (without Docker)

**API (FastAPI):**
```bash
cd api
pip install -r requirements.txt
uvicorn main:app --reload --host 0.0.0.0 --port 8000
```

**Frontend (React/Vite):**
```bash
cd frontend
npm install
npm run dev      # Dev server at http://localhost:5173
npm run build    # Production build
npm run lint     # ESLint
```

**Load Testing:**
```bash
cd load-testing
pip install -r requirements.txt
locust -f locustfile.py --host=http://localhost:8000

# Headless with report
./run_load_test.sh -u 100 -r 10 -t 60s --headless --html
```

### API Tests
```bash
cd api
pytest
pytest -v tests/test_specific.py  # Single test file
```

## Architecture

```
Databricks (Delta Lake) ──Spark Streaming + MongoDB Connector──▶ MongoDB Atlas
                                                                      │
                                                            Change Streams (CDC)
                                                                      ▼
                                                                FastAPI (api/)
                                                                      │
                                                            REST + WebSocket
                                                                      ▼
                                                             React Dashboard
```

### Two CDC Mechanisms
1. **Delta Lake Change Data Feed** - Databricks → MongoDB (notebook 03 uses Spark Connector)
2. **MongoDB Change Streams** - MongoDB → FastAPI → Frontend (real-time updates via WebSocket)

## Key Code Locations

### API (`api/`)
- `main.py` - FastAPI app with lifespan management, CORS, timing middleware
- `config.py` - Pydantic settings from environment variables
- `services/mongodb.py` - Motor async MongoDB client, all database operations
- `services/change_streams.py` - MongoDB Change Streams for real-time CDC
- `routers/` - REST endpoints: accounts, transactions, search (Atlas Search), metrics
- `websocket/changes.py` - WebSocket endpoint for pushing changes to clients

### Frontend (`frontend/`)
- React 18 + Vite + Tailwind CSS
- `src/App.jsx` - Main app with tab navigation (Overview, Transactions)
- `src/components/` - SyncMonitor, AccountLookup, TransactionExplorer, ChangeStreamFeed

### Databricks Notebooks (`databricks/`)
- `01_setup_delta_tables.py` - Creates Delta Lake schema (run once)
- `02_data_generator.py` - Generates test banking data (run on demand)
- `03_streaming_to_mongodb.py` - Streams changes to MongoDB using Spark Connector (keep running)

**MongoDB Spark Connector usage (notebook 03):**
```python
df.write \
    .format("mongodb") \
    .option("connection.uri", mongodb_uri) \
    .option("database", "banking_odl") \
    .option("collection", "account_statements") \
    .option("upsertDocument", "true") \
    .save()
```

### Load Testing (`load-testing/`)
- `locustfile.py` - Two user classes: BankingAPIUser (realistic), HighThroughputUser (max QPS)
- Auto-enables benchmark mode on test start (runs explain-only queries)
- Reports actual MongoDB `executionTimeMillis` (from `_dbExecTimeMs` in response body) instead of network round-trip
- Automatically verifies SLA targets at test completion
- Access at http://localhost:8089 when docker-compose is running

## MongoDB Schema

Collection: `account_statements` in `banking_odl` database

Key fields (camelCase):
- `accountNumber` - Primary identifier
- `accountHolder` - Nested: name, email, phone, address
- `transactions` - Array of transaction objects
- `statementPeriod.startDate/endDate` - Date range
- `closingBalance` - Current balance

Indexes required: See `atlas-setup/create_indexes.js`
Atlas Search index: See `atlas-setup/create_search_index.json`

## Environment Variables

Required:
- `MONGODB_URI` - MongoDB Atlas connection string

Optional:
- `MONGODB_DATABASE` - Default: `banking_odl`
- `API_PORT` - Default: `8000`
- `VITE_API_URL` - Frontend API URL (default: `http://localhost:8000`)
