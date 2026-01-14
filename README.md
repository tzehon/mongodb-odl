# MongoDB Atlas Operational Data Layer (ODL) Demo

Real-time data streaming from Databricks Lakehouse to MongoDB Atlas for banking account statements.

## Quick Start Checklist

Follow these steps in order:

| Step | Task | Time | Guide |
|------|------|------|-------|
| 1 | Create MongoDB Atlas cluster | 10 min | [Section 1](#step-1-mongodb-atlas-setup) |
| 2 | Create indexes and search index | 5 min | [Section 1.5](#15-create-indexes) |
| 3 | Setup Databricks compute | 10 min | [databricks/SETUP.md](databricks/SETUP.md) |
| 4 | Install MongoDB Spark Connector | 5 min | [databricks/SETUP.md](databricks/SETUP.md#4-install-mongodb-spark-connector) |
| 5 | Run Databricks notebooks | 10 min | [databricks/SETUP.md](databricks/SETUP.md#6-import-and-run-notebooks) |
| 6 | Run docker-compose | 2 min | [Section 3](#step-3-run-the-demo-application) |

**Total setup time: ~45 minutes**

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                     DATABRICKS LAKEHOUSE                        │
│                      (Delta Lake Tables)                        │
│                              │                                   │
│              Spark Structured Streaming                          │
│              MongoDB Connector v10.5                             │
└──────────────────────────────┼───────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────┐
│                    MONGODB ATLAS (ODL)                          │
│  • account_statements collection                                 │
│  • Compound indexes for OLTP queries                            │
│  • Atlas Search for full-text                                   │
│  • Change Streams for CDC                                       │
└──────────────────────────────┼───────────────────────────────────┘
                               │
              ┌────────────────┼────────────────┐
              │                │                │
              ▼                ▼                ▼
         FastAPI API     React Dashboard    Locust
         (Port 8000)      (Port 3000)     (Port 8089)
```

### Target SLA

- **500 QPS** (queries per second)
- **< 100ms p95 latency**

### How the Components Connect

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              FULL DATA FLOW                                  │
└─────────────────────────────────────────────────────────────────────────────┘

  DATABRICKS LAKEHOUSE                          MONGODB ATLAS (ODL)
  ┌─────────────────────┐                      ┌─────────────────────┐
  │ Delta Lake          │   Spark Streaming   │ account_statements  │
  │ (source of truth)   │ ─────────────────▶  │ collection          │
  │                     │   + MongoDB Spark   │                     │
  │                     │     Connector       │                     │
  └─────────────────────┘                      └─────────────────────┘
                                                         │
                                                         │ Change Streams (CDC)
                                                         ▼
                                               ┌─────────────────────┐
                                               │ FastAPI Application │
                                               │ (REST + WebSocket)  │
                                               └─────────────────────┘
                                                         │
                                                         │ HTTP / WebSocket
                                                         ▼
                                               ┌─────────────────────┐
                                               │ React Dashboard     │
                                               └─────────────────────┘
```

### Two "Change" Mechanisms

| Mechanism | Direction | Purpose |
|-----------|-----------|---------|
| **Delta Lake Change Data Feed** | Databricks → MongoDB | Streams new/updated data to Atlas |
| **MongoDB Change Streams** | MongoDB → App | Pushes real-time updates to frontend |

```
Delta Lake ──CDF──▶ Spark ──Connector──▶ MongoDB ──Change Streams──▶ FastAPI ──WebSocket──▶ Browser
```

### Pipeline Latency Measurement

The dashboard shows **Pipeline Latency** - the actual time Spark takes to process a batch and write to MongoDB.

```
                    TIMELINE
    ────────────────────────────────────────────────────────────────▶ time

    T0              T1                      T2
    │               │                       │
    ▼               ▼                       ▼
┌───────┐     ┌───────────┐          ┌───────────┐     ┌──────────────┐
│ Data  │     │  Spark    │          │  MongoDB  │     │   Dashboard  │
│Created│     │  Batch    │          │  Write    │────▶│ (via Change  │
│       │     │ Starts    │          │ Complete  │     │   Streams)   │
└───────┘     └───────────┘          └───────────┘     └──────────────┘
    │               │                       │                  │
    │◄─────────────►│◄─────────────────────►│                  │
    │ Trigger Wait  │     Pipeline          │    Real-time     │
    │ (excluded)    │     Latency           │    (no polling)  │
    │               │     (measured)        │                  │
```

**How it works:**
1. **T0**: Data created in Delta Lake (has `generatedAt` timestamp)
2. **T1**: Spark batch starts processing (captured as `_batchStartTime`)
3. **T2**: Spark writes to MongoDB (captured as `_processedAt`)
4. **Dashboard**: Receives update via Change Streams (real-time, no polling)

**Pipeline Latency = T2 - T1** (actual Spark processing + MongoDB write time)

This measures exactly how long Spark takes to process and write data:
- Excludes trigger interval wait time (T0 → T1)
- Change Streams push updates to dashboard in real-time (no polling delay)

---

## Step 1: MongoDB Atlas Setup

### 1.1 Create Atlas Cluster

1. Go to [cloud.mongodb.com](https://cloud.mongodb.com) and sign up/login
2. Create a new project: `ODL-Demo`
3. Click **Build a Database**
4. Configure:
   - **Deployment Type**: Dedicated
   - **Tier**: M10 (minimum) or M30 (recommended for load testing)
   - **Cloud Provider**: AWS
   - **Region**: Singapore (ap-southeast-1)
   - **Cluster Name**: `odl-banking-sg`
5. Click **Create Deployment** and wait 3-5 minutes

### 1.2 Configure Network Access

1. Go to **Network Access** in left sidebar
2. Click **Add IP Address**
3. Click **Allow Access from Anywhere** (adds 0.0.0.0/0)
4. Click **Confirm**

### 1.3 Create Database User

1. Go to **Database Access** in left sidebar
2. Click **Add New Database User**
3. Configure:
   - **Username**: `odl_app_user`
   - **Password**: Generate a strong password (save this!)
   - **Privileges**: Read and write to any database
4. Click **Add User**

### 1.4 Get Connection String

1. Go to **Database** in left sidebar
2. Click **Connect** on your cluster
3. Select **Drivers**
4. Copy the connection string:
   ```
   mongodb+srv://odl_app_user:<password>@odl-banking-sg.xxxxx.mongodb.net/?retryWrites=true&w=majority
   ```
5. Replace `<password>` with your password
6. Add database name after `.net/`:
   ```
   mongodb+srv://odl_app_user:YOUR_PASSWORD@odl-banking-sg.xxxxx.mongodb.net/banking_odl?retryWrites=true&w=majority
   ```

**Save this connection string - you'll need it for Databricks and docker-compose.**

### 1.5 Create Indexes

Using MongoDB Shell (mongosh):

```bash
mongosh "mongodb+srv://odl-banking-sg.xxxxx.mongodb.net/banking_odl" \
  --username odl_app_user \
  --file atlas-setup/create_indexes.js
```

Or use MongoDB Compass to connect and create indexes manually.

### 1.6 Create Atlas Search Index

1. In Atlas UI, go to **Database** → **Atlas Search**
2. Click **Create Search Index**
3. Select **JSON Editor**
4. Database: `banking_odl`, Collection: `account_statements`
5. Paste contents of `atlas-setup/create_search_index.json`
6. Click **Create Search Index**

---

## Step 2: Databricks Setup

Follow the detailed guide: **[databricks/SETUP.md](databricks/SETUP.md)**

### Summary

1. **Create Databricks workspace** (AWS/Azure/GCP)

2. **Create compute** (Single Node recommended for demo)
   - Runtime: Any LTS with Scala 2.12 (e.g., 13.3 LTS, 16.4 LTS)
   - Access Mode: **Dedicated (Single user)** - required for custom libraries
   - Node: m5.xlarge (AWS) or Standard_D4ds_v4 (Azure)

3. **Install MongoDB Spark Connector**
   - Go to Compute → Your Compute → Libraries → Install new → Maven
   - Coordinates: `org.mongodb.spark:mongo-spark-connector_2.12:10.5.0`

4. **Run notebooks in order**
   - `01_setup_delta_tables.py` - Create Delta tables
   - `02_data_generator.py` - Generate banking data
   - `03_streaming_to_mongodb.py` - Start streaming to Atlas (enter MongoDB URI in widget)

---

## Step 3: Run the Demo Application

### 3.1 Configure Environment

```bash
cd mongodb-odl-demo
cp .env.example .env
```

Edit `.env` and set your MongoDB URI:

```
MONGODB_URI=mongodb+srv://odl_app_user:YOUR_PASSWORD@odl-banking-sg.xxxxx.mongodb.net/banking_odl?retryWrites=true&w=majority
```

### 3.2 Start Services

```bash
docker-compose up --build
```

### 3.3 Access Applications

| Application | URL |
|-------------|-----|
| Dashboard | http://localhost:3000 |
| API Docs | http://localhost:8000/docs |
| Load Testing | http://localhost:8089 |

---

## Running the Demo

### Target SLA to Prove

| SLA | Target | How to Prove |
|-----|--------|--------------|
| **Data Sync Latency** | < 10 seconds | Sync Monitor + Change Stream Feed |
| **Query Throughput** | 500 QPS | Locust Load Test |
| **Query Latency** | < 100ms p95 | Locust Load Test |

---

### Proving Real-Time Data Sync (< 15 seconds)

This proves data flows from Databricks Delta Lake to MongoDB Atlas in seconds.

> **Note**: This demo shows **one-way sync** (Lakehouse → Atlas). The dashboard reads from MongoDB using Change Streams, but does not write back to Databricks.

#### Prerequisites

- [ ] Notebook `03_streaming_to_mongodb.py` is running in Databricks (streaming active)
- [ ] Docker-compose is running (`docker-compose up --build`)
- [ ] Initial data exists in MongoDB (from running notebook 02 earlier)

#### Setup: Two Windows Side-by-Side

| Window | What to Open |
|--------|--------------|
| **Left: Dashboard** | http://localhost:3000 → **Sync Monitor** tab |
| **Right: Databricks** | Notebook `02_data_generator.py` |

#### Step-by-Step Demo

**Step 1: Note the starting state** (Left window - Dashboard)

Look at the Sync Monitor and note:
- Document count (e.g., 100)
- Last sync timestamp

**Step 2: Configure notebook 02** (Right window - Databricks)

Change the widgets at the top of the notebook:

| Widget | Value | Why |
|--------|-------|-----|
| `num_accounts` | `10` | Smaller batch for quick demo |
| `transactions_per_statement` | `50` | Default is fine |
| `mode` | `batch` | Generates once and stops |

**Step 3: Run notebook 02**

Click **Run All** at the top of the notebook. You'll see:
```
Generating 10 account statements in batch mode...
Generated 10/10 statements...
Successfully wrote 10 statements to Delta table
```

**Step 4: Watch the Dashboard** (Left window)

Within **5-10 seconds**, you'll see:
- Document count: 100 → **110** (increased by 10)
- Last sync timestamp: **Just now**
- Sync lag: **< 15 seconds**

**Step 5: (Optional) Show Change Stream Feed**

1. Click **Change Stream Feed** tab in the Dashboard
2. Run notebook 02 again in Databricks
3. Watch real-time events appear as each document syncs

#### Demo Script

```
"Here's our Databricks notebook generating banking data..."
→ Run notebook 02 (10 accounts)

"Watch the dashboard - within seconds the data syncs..."
→ Point to Sync Monitor showing count increasing

"Data flows from Databricks Lakehouse to MongoDB Atlas in under 15 seconds"
→ Highlight the sync lag metric

"MongoDB Change Streams let us see each document as it arrives in real-time"
→ Show Change Stream Feed tab
```

#### API Verification

```bash
# Check sync status
curl http://localhost:8000/api/v1/sync/status
```

Expected response:
```json
{
  "document_count": 1050,
  "last_updated": "2024-01-15T10:30:45Z",
  "sync_lag_seconds": 3.2
}
```

#### What to Highlight

- "Data written to Databricks appears in MongoDB within **5-10 seconds**"
- "Change Streams provide **real-time CDC** - no polling required"
- "The streaming notebook checks for changes every 5 seconds (configurable)"

---

### Proving Query Performance (500 QPS, < 100ms p95)

This proves MongoDB Atlas can handle high throughput with low latency.

#### Option 1: Locust Web UI (Recommended for Live Demos)

1. **Open Locust**: http://localhost:8089

2. **Pre-configured settings** (from `locust.conf`):
   | Setting | Value |
   |---------|-------|
   | Number of users | 200 |
   | Spawn rate | 50 |
   | Host | `http://api:8000` |
   | Run time | 30s |

3. **Click "Start Swarming"** - Benchmark mode is auto-enabled

4. **Watch the real-time charts**:
   - **Charts tab** → Look at "Total Requests per Second" (should reach 500+)
   - **Statistics tab** → Check P95 response times (should be < 100ms)
   - Response times shown are actual MongoDB `executionTimeMillis` from explain queries

5. Test auto-stops after 30 seconds

#### Option 2: Command Line (Automated Testing)

```bash
cd load-testing
./run_load_test.sh -u 200 -r 50 -t 30s --headless --html
```

#### Reading the Results

The test prints an SLA verification summary:

```
============================================================
Load Test Complete - Results Summary
============================================================
Total Requests: 30000
Failure Rate: 0.00%
P95 Response Time: 85.00ms
Actual QPS: 520.00

------------------------------------------------------------
SLA Verification:
  QPS Target (500): PASS (520.00)
  P95 Latency (<100ms): PASS (85.00ms)
  Overall: PASS
============================================================
```

#### Key Metrics to Point Out

| Metric | Where to Find | Target |
|--------|---------------|--------|
| **RPS** | Charts tab → "Total Requests per Second" | ≥ 500 |
| **P95 Latency** | Statistics tab → "95%ile" column | < 100ms |
| **Failure Rate** | Statistics tab → "Failures" column | 0% |

> **Note**: Response times shown are actual MongoDB execution time (`executionTimeMillis` from explain), excluding network latency.

#### What to Highlight

- "MongoDB Atlas handles **500+ queries per second** with ease"
- "**P95 latency under 100ms** - 95% of requests complete in under 100ms"
- "This is with compound indexes - no special tuning required"
- "Can scale further with Atlas sharding if needed"

---

### Demo Flow Summary (20 minutes)

| Step | What to Show | Time |
|------|--------------|------|
| 1 | Architecture overview (show the diagram) | 2 min |
| 2 | **Real-time sync demo** (Sync Monitor + data generator) | 5 min |
| 3 | Account queries (balance, transactions, filters) | 5 min |
| 4 | Full-text search ("fairprice", "grab") | 3 min |
| 5 | Change Stream Feed (real-time events) | 3 min |
| 6 | **Load test demo** (Locust → prove 500 QPS, <100ms) | 5 min |

---

## API Reference

### Accounts
```
GET /api/v1/accounts
GET /api/v1/accounts/{account}/balance
GET /api/v1/accounts/{account}/statements/latest
```

### Transactions
```
GET /api/v1/accounts/{account}/transactions
GET /api/v1/accounts/{account}/transactions?category=groceries
GET /api/v1/accounts/{account}/transactions/search?q=fairprice
```

### Real-time
```
WebSocket /ws/changes
WebSocket /ws/changes/{account}
```

### Metrics
```
GET /api/v1/metrics
GET /api/v1/sync/status
```

---

## Load Testing

### Locust Web UI
1. Open http://localhost:8089 (available when docker-compose is running)
2. Default config: 200 users, spawn rate 50, 30s run time (pre-configured in `locust.conf`)
3. Click **Start Swarming**

**Benchmark Mode**: Locust automatically enables benchmark mode when the test starts. This:
- Runs explain-only queries against MongoDB
- Reports actual MongoDB `executionTimeMillis` (DB execution time, not network round-trip)
- Response times shown in Locust are true MongoDB server-side latency

### Command Line
```bash
cd load-testing
./run_load_test.sh -u 200 -r 50 -t 30s --headless --html
```

---

## Troubleshooting

### API not responding
```bash
docker-compose logs api
# Check MONGODB_URI in .env
```

### No data in dashboard
Verify streaming is running in Databricks notebook `03_streaming_to_mongodb.py`

### Connection timeout
Check Atlas Network Access includes 0.0.0.0/0 or your IP

### Databricks: Library permission denied
Create compute with **Access Mode: Dedicated (Single user)** - shared mode requires allowlist.

### Databricks: ClassNotFoundException
Install MongoDB Spark Connector:
- Compute → Your Compute → Libraries → Install new → Maven
- `org.mongodb.spark:mongo-spark-connector_2.12:10.5.0`

For more Databricks troubleshooting, see [databricks/SETUP.md](databricks/SETUP.md#7-troubleshooting)

---

## Project Structure

```
mongodb-odl-demo/
├── README.md                 # This file (start here)
├── docker-compose.yml
├── .env.example
├── databricks/
│   ├── SETUP.md              # Detailed Databricks guide
│   ├── 01_setup_delta_tables.py
│   ├── 02_data_generator.py
│   └── 03_streaming_to_mongodb.py
├── atlas-setup/
│   ├── create_indexes.js
│   └── create_search_index.json
├── api/                      # FastAPI backend
├── frontend/                 # React dashboard
└── load-testing/             # Locust tests
```
