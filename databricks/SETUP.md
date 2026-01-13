# Databricks Setup Guide for MongoDB Atlas ODL Demo

This guide provides step-by-step instructions for setting up Databricks to stream data to MongoDB Atlas.

> **Note**: This demo requires a Databricks workspace deployed on AWS, Azure, or GCP. Databricks Free Edition does not support custom cluster libraries required for the MongoDB Spark Connector.

## Table of Contents

1. [Cost Estimates](#1-cost-estimates)
2. [Create Databricks Workspace](#2-create-databricks-workspace)
3. [Create Compute](#3-create-compute)
4. [Install MongoDB Spark Connector](#4-install-mongodb-spark-connector)
5. [Configure MongoDB Connection](#5-configure-mongodb-connection)
6. [Import and Run Notebooks](#6-import-and-run-notebooks)
7. [Troubleshooting](#7-troubleshooting)

---

## 1. Cost Estimates

Understanding Databricks costs helps you plan and budget for running this demo.

### Pricing Model

Databricks charges based on **Databricks Units (DBUs)**, which measure processing capability per hour. You also pay for the underlying cloud infrastructure (VMs, storage).

**Total Cost = DBU Cost + Cloud Infrastructure Cost**

### Demo Compute Costs

For a minimal demo compute running the streaming workload:

| Component | Configuration | DBU/Hour | Cloud Cost/Hour | Total/Hour |
|-----------|--------------|----------|-----------------|------------|
| **Single Node (Dev)** | 4 vCPU, 16 GB | ~0.4 DBU | ~$0.15-0.20 | **~$0.40-0.50** |
| **Small Multi-Node** | 1 driver + 2 workers | ~1.2 DBU | ~$0.45-0.60 | **~$1.20-1.50** |
| **Standard Multi-Node** | 1 driver + 4 workers | ~2.0 DBU | ~$0.75-1.00 | **~$2.00-2.50** |

> **Note**: Prices vary by cloud provider, region, and Databricks tier. Above estimates are approximate for AWS/Azure in US regions.

### Estimated Demo Costs

| Scenario | Duration | Estimated Cost |
|----------|----------|----------------|
| **Quick demo** (single node) | 1 hour | $0.50 - $1.00 |
| **Half-day workshop** | 4 hours | $2.00 - $4.00 |
| **Full-day workshop** | 8 hours | $4.00 - $8.00 |
| **Left running overnight** | 12 hours | $6.00 - $12.00 |

### Cost Optimization Tips

1. **Use auto-termination**: Set compute to terminate after 30-60 minutes of inactivity
2. **Use Single Node mode**: Sufficient for demo workloads, ~70% cheaper than multi-node
3. **Stop compute when not in use**: Manually terminate between demo sessions
4. **Use spot/preemptible instances**: 60-90% cheaper for non-critical workloads (configure in Advanced Options)
5. **Choose smaller instance types**: `m5.large` (AWS) or `Standard_D2ds_v4` (Azure) works for basic demos

### Free Trial Options

| Provider | Trial Offer |
|----------|-------------|
| **AWS** | $400 Databricks credit (14 days) via AWS Marketplace |
| **Azure** | $200 Azure credit (30 days) for new accounts + Databricks trial |
| **GCP** | $300 GCP credit (90 days) for new accounts + Databricks trial |

### Monitor Your Costs

1. **Databricks Admin Console**: View DBU usage under **Settings > Usage**
2. **Cloud Provider Console**: Monitor VM and storage costs
3. **Set billing alerts**: Configure alerts in your cloud provider to avoid surprises

---

## 2. Create Databricks Workspace

### AWS

1. Log in to your AWS Console
2. Navigate to **AWS Marketplace** and search for "Databricks"
3. Subscribe to Databricks and follow the setup wizard
4. Alternatively, use the [Databricks Account Console](https://accounts.cloud.databricks.com):
   - Create a new workspace
   - Select AWS as the cloud provider
   - Choose your region (recommend `ap-southeast-1` for Singapore to minimize latency to Atlas)
   - Configure VPC settings (use default or custom)

### Azure

1. Log in to Azure Portal
2. Search for "Azure Databricks" in the marketplace
3. Click **Create**
4. Configure:
   - **Subscription**: Your Azure subscription
   - **Resource Group**: Create new or use existing
   - **Workspace Name**: `odl-demo-workspace`
   - **Region**: Southeast Asia (Singapore)
   - **Pricing Tier**: Premium (required for Unity Catalog)
5. Click **Review + Create**, then **Create**

### GCP

1. Log in to Google Cloud Console
2. Navigate to **Marketplace** and search for "Databricks"
3. Click **Subscribe** and follow the setup wizard
4. Or use Databricks Account Console to create a GCP workspace

---

## 3. Create Compute

> **Note**: Databricks now uses "Compute" instead of "Cluster" in the UI, but the terms are interchangeable.

### Navigate to Compute

1. In your Databricks workspace, click **Compute** in the left sidebar
2. Click **Create compute** (or use **New > Cluster** from the sidebar)

### Compute Configuration

#### Recommended Settings for Demo (Single Node)

| Setting | Value |
|---------|-------|
| **Compute name** | `odl-streaming-cluster` |
| **Policy** | Unrestricted |
| **Single node** | ✅ Checked (recommended for demos) |
| **Databricks runtime** | Any LTS version with **Scala 2.12** (e.g., 13.3 LTS, 14.3 LTS, 15.4 LTS, 16.4 LTS) |
| **Node type** | See cloud-specific options below |
| **Terminate after** | 60 minutes of inactivity |

> **Important**: Ensure the runtime includes **Scala 2.12** to match the MongoDB Spark Connector. Check the runtime description shows "Scala 2.12".

#### Access Mode (Required for Unity Catalog Workspaces)

If your workspace has Unity Catalog enabled, you must configure the access mode to install custom libraries:

1. Expand the **Advanced** section at the bottom
2. Find **Access mode**
3. Change from "Standard (formerly: Shared)" to **"Dedicated (formerly: Single user)"**

| Access Mode | Can Install Custom Libraries? |
|-------------|------------------------------|
| Standard (formerly: Shared) | ❌ No - requires allowlist |
| **Dedicated (formerly: Single user)** | ✅ Yes - recommended |
| No isolation shared | ✅ Yes |

> **Why this matters**: Unity Catalog restricts Maven library installation on shared compute. Using "Dedicated" mode bypasses the allowlist requirement and allows installing the MongoDB Spark Connector.

#### Node Types by Cloud Provider

| Cloud | Recommended Type | Specs |
|-------|------------------|-------|
| **AWS** | `m5.large` or `m5.xlarge` | 2-4 vCPUs, 8-16 GB RAM |
| **Azure** | `Standard_D2ds_v4` or `Standard_D4ds_v4` | 2-4 vCPUs, 8-16 GB RAM |
| **GCP** | `n1-standard-2` or `n1-standard-4` | 2-4 vCPUs, 7.5-15 GB RAM |

#### Multi-Node Configuration (Optional)

For production or larger workloads, uncheck "Single node" and configure:

```
Single node: ☐ Unchecked
Preferred worker type: Standard_D4ds_v4 (Azure) / m5.xlarge (AWS)
Min workers: 1
Max workers: 2-4
Enable autoscaling: ✅ Checked
```

> **Quota Warning**: If you see "This account may not have enough CPU cores", either:
> - Use **Single node** mode (recommended for demo)
> - Reduce **Max workers** to fit within your quota
> - Request a quota increase from your cloud provider

### Advanced Options (Optional)

Click **Advanced** to expand, then under **Spark Config**, add:

```
spark.sql.shuffle.partitions 8
spark.streaming.stopGracefullyOnShutdown true
spark.mongodb.output.batchSize 512
```

### Create the Compute

Click **Create compute** and wait for it to start (usually 3-5 minutes).

---

## 4. Install MongoDB Spark Connector

The MongoDB Spark Connector must be installed as a compute library.

### Step-by-Step Installation

1. **Navigate to your compute**:
   - Click **Compute** in the left sidebar
   - Click on your compute name (`odl-streaming-cluster`)

2. **Go to Libraries tab**:
   - Click the **Libraries** tab at the top of the compute details page

3. **Install new library**:
   - Click **Install new**

4. **Select Maven as library source**:
   - In the dialog, select **Maven** as the Library Source

5. **Enter Maven coordinates**:
   - In the **Coordinates** field, enter:
   ```
   org.mongodb.spark:mongo-spark-connector_2.12:10.5.0
   ```

   **Breaking down the coordinates:**
   - `org.mongodb.spark` - Group ID (organization)
   - `mongo-spark-connector_2.12` - Artifact ID (the `_2.12` indicates Scala 2.12 compatibility)
   - `10.5.0` - Version number

6. **Optional: Add Repository** (usually not needed):
   - Leave the Repository field empty (Maven Central is used by default)

7. **Click Install**:
   - Click **Install** to add the library
   - The status will show "Pending" then "Installing"
   - Wait for status to change to **Installed**

8. **Restart compute if needed**:
   - If the library was installed on a running compute, you may need to restart
   - Click **Restart** if prompted

### Verify Installation

After installation, you can verify the connector is available by running this in a notebook:

```python
# Verify MongoDB Spark Connector is available
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# This should not throw an error
spark._jvm.com.mongodb.spark.sql.connector.MongoTableProvider
print("MongoDB Spark Connector is installed correctly!")
```

### Alternative: Install via Init Script

For automated compute setup, create an init script:

```bash
#!/bin/bash
# init_mongodb_connector.sh

# Download MongoDB Spark Connector
wget -q https://repo1.maven.org/maven2/org/mongodb/spark/mongo-spark-connector_2.12/10.5.0/mongo-spark-connector_2.12-10.5.0.jar \
  -O /databricks/jars/mongo-spark-connector_2.12-10.5.0.jar

# Download dependencies
wget -q https://repo1.maven.org/maven2/org/mongodb/mongodb-driver-sync/4.11.1/mongodb-driver-sync-4.11.1.jar \
  -O /databricks/jars/mongodb-driver-sync-4.11.1.jar
wget -q https://repo1.maven.org/maven2/org/mongodb/mongodb-driver-core/4.11.1/mongodb-driver-core-4.11.1.jar \
  -O /databricks/jars/mongodb-driver-core-4.11.1.jar
wget -q https://repo1.maven.org/maven2/org/mongodb/bson/4.11.1/bson-4.11.1.jar \
  -O /databricks/jars/bson-4.11.1.jar
```

### How the Spark Connector is Used in Code

The MongoDB Spark Connector is used in `03_streaming_to_mongodb.py` to write data to MongoDB Atlas. Here's the key code:

```python
# ============================================================
# MONGODB SPARK CONNECTOR IN ACTION
# File: databricks/03_streaming_to_mongodb.py (lines 253-263)
# ============================================================

# Write DataFrame to MongoDB using the Spark Connector
final_df.write \
    .format("mongodb") \                              # <-- Uses MongoDB Spark Connector
    .option("connection.uri", mongodb_uri) \          # MongoDB Atlas connection string
    .option("database", mongodb_database) \           # Target database: banking_odl
    .option("collection", mongodb_collection) \       # Target collection: account_statements
    .option("operationType", "update") \              # Upsert mode
    .option("upsertDocument", "true") \               # Insert if not exists, update if exists
    .option("idFieldList", "accountNumber,statementPeriod.startDate") \  # Composite key
    .mode("append") \
    .save()
```

**Key Points:**
- `.format("mongodb")` - This tells Spark to use the MongoDB Spark Connector
- The connector handles connection pooling, batching, and retries automatically
- `upsertDocument: true` ensures idempotent writes (safe to replay)
- `idFieldList` defines the composite key for upserts

**Without the connector installed**, you would see:
```
java.lang.ClassNotFoundException: com.mongodb.spark.sql.connector.MongoTableProvider
```

---

## 5. Configure MongoDB Connection

For this demo, we'll use **notebook widgets** to pass the MongoDB connection string at runtime. This is the simplest approach and avoids storing credentials in code or requiring additional setup.

> **Note**: For production deployments, consider using Databricks Secrets backed by Azure Key Vault or another secrets manager.

### How Notebook Widgets Work

The streaming notebook will prompt you for the MongoDB URI when you run it:

```python
# Widget creates an input field at the top of the notebook
dbutils.widgets.text("mongodb_uri", "", "MongoDB Atlas URI")

# Retrieve the value when running
mongodb_uri = dbutils.widgets.get("mongodb_uri")
```

### Your MongoDB Connection String

You'll need your MongoDB Atlas connection string in this format:

```
mongodb+srv://<username>:<password>@<cluster>.mongodb.net/<database>?retryWrites=true&w=majority
```

**Example:**
```
mongodb+srv://odl_app_user:MyPassword123@odl-cluster.abc123.mongodb.net/banking_odl?retryWrites=true&w=majority
```

### Where to Find Your Connection String

1. Go to [MongoDB Atlas](https://cloud.mongodb.com)
2. Navigate to your cluster
3. Click **Connect** → **Drivers**
4. Copy the connection string and replace `<password>` with your actual password

### Security Notes

- The widget value is **not saved** in the notebook file
- You'll need to re-enter it each time you run the notebook
- **Never commit notebooks with hardcoded connection strings** to version control

---

## 6. Import and Run Notebooks

### Understanding the Demo Architecture

This demo uses **Delta Lake** as the source and streams data to **MongoDB Atlas**.

#### What is a Lakehouse?

A **Lakehouse** combines the best of data warehouses and data lakes:

| Traditional Approach | Lakehouse Approach |
|---------------------|-------------------|
| **Data Lake**: Cheap storage, but no reliability | ✅ Reliable ACID transactions |
| **Data Warehouse**: Reliable, but expensive & limited formats | ✅ Open formats (Parquet) on cheap storage |
| Separate systems for analytics and ML | ✅ Single platform for all workloads |

Databricks pioneered the Lakehouse architecture, using **Delta Lake** as the storage layer that adds reliability to data lakes.

#### What is Delta Lake?

Delta Lake is an open-source storage layer that brings reliability to data lakes. It provides:

- **ACID transactions** - Ensures data integrity with atomic commits
- **Schema enforcement** - Prevents bad data from corrupting your tables
- **Time travel** - Query historical versions of your data
- **Change Data Feed (CDF)** - Track row-level changes for streaming

In this demo, Delta Lake acts as the **source of truth** in the Databricks Lakehouse, and changes are streamed to MongoDB Atlas for operational workloads.

#### The Three Notebooks

| Notebook | Purpose | What It Does |
|----------|---------|--------------|
| **01_setup_delta_tables.py** | Create schema | Creates the Delta Lake tables (`account_statements`, `transactions_stream`) with proper schema, partitioning, and Change Data Feed enabled |
| **02_data_generator.py** | Generate test data | Creates realistic Singapore banking data (accounts, transactions, merchants) and writes to Delta tables. Supports batch or continuous generation modes |
| **03_streaming_to_mongodb.py** | Stream to MongoDB | Reads changes from Delta Lake using Change Data Feed and streams them to MongoDB Atlas using the Spark Connector. Handles upserts and transforms data to MongoDB's camelCase schema |

#### Data Flow

```
┌──────────────────┐      ┌──────────────────┐      ┌──────────────────┐
│ 02_data_generator│─────▶│   Delta Lake     │      │  MongoDB Atlas   │
│ (writes data)    │      │ (stores data)    │      │ (serves apps)    │
└──────────────────┘      └──────────────────┘      └──────────────────┘
                                   │                         ▲
                                   │ Change Data Feed        │ MongoDB Spark
                                   │ (tracks changes)        │ Connector (writes)
                                   ▼                         │
                          ┌──────────────────┐               │
                          │ Spark Structured │───────────────┘
                          │ Streaming        │
                          │ (03_streaming_..)│
                          └──────────────────┘
```

#### How the Components Work Together

| Component | What It Is | Role in This Demo |
|-----------|-----------|-------------------|
| **Delta Lake** | Storage layer in Databricks | Stores banking data with ACID transactions |
| **Change Data Feed** | Delta Lake feature | Tracks which rows were inserted/updated/deleted |
| **Spark Structured Streaming** | Spark's streaming engine | Continuously reads changes from Delta Lake |
| **MongoDB Spark Connector** | Library (JAR) | Writes data from Spark to MongoDB Atlas |
| **MongoDB Atlas** | Cloud database | Serves data to applications via ODL |

**Key point**: Delta Lake and MongoDB Atlas don't connect directly. Notebook 03 uses **Spark Structured Streaming** to read changes (via Change Data Feed) and the **MongoDB Spark Connector** to write them to Atlas.

#### How Does the Streaming Work?

The streaming runs **continuously** using a micro-batch pattern:

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         Streaming Loop (runs forever)                   │
│                                                                         │
│   ┌─────────┐     ┌─────────┐     ┌─────────┐     ┌─────────┐          │
│   │ Batch 1 │────▶│ Batch 2 │────▶│ Batch 3 │────▶│ Batch 4 │───▶ ...  │
│   │ 10 sec  │     │ 10 sec  │     │ 10 sec  │     │ 10 sec  │          │
│   └─────────┘     └─────────┘     └─────────┘     └─────────┘          │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

1. **Every trigger interval** (default: 5 seconds), Spark checks Delta Lake for new changes
2. **If changes exist**, it reads them, transforms them, and writes to MongoDB
3. **If no changes**, it waits for the next interval
4. **Checkpoint** tracks progress - if the notebook restarts, it resumes where it left off
5. **Runs until you stop it** by calling `query.stop()` or terminating the compute

| Setting | What It Controls |
|---------|------------------|
| `trigger_interval` | How often to check for changes (e.g., "5 seconds") |
| `checkpoint_path` | Where to save progress (survives restarts) |
| `start_fresh` | If true, ignores checkpoint and reprocesses all data |

**To stop streaming**: Run `query.stop()` in the notebook, or terminate the compute.

#### What Does the Python Code Do?

The notebook 03 code does the following:

```python
# 1. READ: Set up a streaming read from Delta Lake with Change Data Feed
delta_stream = spark.readStream \
    .format("delta") \
    .option("readChangeFeed", "true") \    # Track changes only
    .table("account_statements")

# 2. PROCESS: For each micro-batch, transform and write to MongoDB
def write_to_mongodb(batch_df, batch_id):
    # Filter for inserts/updates (ignore deletes)
    actionable = batch_df.filter(col("_change_type").isin(["insert", "update_postimage"]))

    # Transform snake_case to camelCase for MongoDB
    transformed = transform_for_mongodb(actionable)

    # Write to MongoDB using the Spark Connector
    transformed.write \
        .format("mongodb") \
        .option("connection.uri", mongodb_uri) \
        .option("database", "banking_odl") \
        .option("collection", "account_statements") \
        .save()

# 3. RUN: Start the streaming query (runs forever)
query = delta_stream \
    .writeStream \
    .foreachBatch(write_to_mongodb) \      # Call our function for each batch
    .option("checkpointLocation", path) \  # Track progress
    .trigger(processingTime="5 seconds") \# Check every 5 seconds
    .start()
```

**In plain English:**
1. **Read** changes from Delta Lake (not all data, just what changed)
2. **Transform** each batch (rename columns to match MongoDB schema)
3. **Write** to MongoDB Atlas using the Spark Connector
4. **Repeat** every 5 seconds

#### What About MongoDB Change Streams?

**Change Streams** are a MongoDB feature for reading changes *from* MongoDB. They're used in the **FastAPI application** (not Databricks) to push real-time updates to frontend clients.

> See the main [README.md](../README.md) for the full project architecture including the FastAPI application and frontend.

### Import Notebooks

1. **Navigate to Workspace**:
   - Click **Workspace** in the left sidebar
   - Expand **Users** and click on your username folder

2. **Import notebooks**:
   - **Right-click** on your folder in the left sidebar
   - Select **Import**
   - Choose **File** and upload each notebook from the `databricks/` folder:
     - `01_setup_delta_tables.py`
     - `02_data_generator.py`
     - `03_streaming_to_mongodb.py`
   - Or drag and drop the files into the import dialog

### Alternative: Import from Git

1. Click **Repos** in the left sidebar
2. Click **Add Repo**
3. Enter your repository URL
4. Clone the repository
5. Navigate to the `databricks/` folder

### Run Notebooks in Order

#### Step 1: Setup Delta Tables (Run Once)

1. Open `01_setup_delta_tables.py`
2. Attach to your compute (`odl-streaming-cluster`)
3. Configure widgets at the top:
   - `catalog`: `main` (or your Unity Catalog name)
   - `schema`: `banking_odl`
   - `reset_tables`: `false` (set to `true` to recreate)
4. Click **Run All** or run cells individually

#### Step 2: Generate Initial Data (Run Once)

1. Open `02_data_generator.py`
2. Attach to your compute
3. Configure widgets:
   - `num_accounts`: `100` (number of accounts to generate)
   - `transactions_per_statement`: `100` (50-200 transactions each)
   - `mode`: `batch` (generates once and stops)
4. Click **Run All**
5. Verify data in Delta table:
   ```sql
   SELECT COUNT(*) FROM banking_odl.account_statements
   ```

#### Step 3: Start Streaming to MongoDB (Leave Running)

1. Open `03_streaming_to_mongodb.py`
2. Attach to your compute
3. Configure widgets at the top of the notebook:
   - `mongodb_uri`: Paste your MongoDB Atlas connection string (see Section 5)
   - `mongodb_database`: `banking_odl`
   - `mongodb_collection`: `account_statements`
   - `trigger_interval`: `5 seconds` (how often to check for new data)
   - `checkpoint_path`: `/Workspace/Users/<your-email>/checkpoints/odl_streaming` (see note below)
   - `start_fresh`: `false` (set to `true` to reprocess all data)

   > **Note**: If you get a DBFS access error, change `checkpoint_path` to use your Workspace path:
   > `/Workspace/Users/your.email@company.com/checkpoints/odl_streaming`

4. Click **Run All**
5. **Leave this notebook running** - it continuously checks for changes
6. Verify data in MongoDB Atlas:
   ```javascript
   db.account_statements.countDocuments()
   ```

### Understanding Data Generation vs Streaming

> **Important**: Data does NOT automatically generate. The two notebooks have different roles:

| Notebook | Role | Runs... |
|----------|------|---------|
| **02_data_generator** | Creates new data in Delta Lake | Only when YOU run it |
| **03_streaming_to_mongodb** | Moves data from Delta Lake → MongoDB | Continuously (every 5 sec) |

**The 5-second interval** is how often notebook 03 *checks* for new data - not how often data is created.

```
You run notebook 02          Notebook 03 (always running)
        │                              │
        │ creates data                 │ checks every 5 seconds
        ▼                              ▼
   Delta Lake ─────────────────▶ MongoDB Atlas
        │                              │
        │                              │
   No new data?                   Nothing to sync
```

### Demo: Show Real-Time Sync

To demonstrate real-time data flow, run **both notebooks simultaneously**:

1. **Keep notebook 03 running** (streaming to MongoDB)
2. **Open notebook 02 in a new browser tab**
3. Change widgets to continuous mode:
   - `mode`: `continuous`
   - `generation_interval_seconds`: `5` (new data every 5 seconds)
   - `continuous_duration_minutes`: `5` (run for 5 minutes)
4. Click **Run All** on notebook 02
5. Watch new data appear in MongoDB within 5-10 seconds

---

## 7. Troubleshooting

### Common Issues

#### Empty or Invalid MongoDB URI

If you see connection errors, check that you've entered the MongoDB URI in the widget at the top of the notebook. The widget field should contain your full connection string:

```
mongodb+srv://username:password@cluster.mongodb.net/database?retryWrites=true&w=majority
```

#### Connection Timeout to MongoDB

```
com.mongodb.MongoTimeoutException: Timed out after 30000 ms
```

**Solutions**:
1. Verify Atlas Network Access allows Databricks IPs
2. Add `0.0.0.0/0` temporarily for testing (remove for production)
3. Check VPC Peering configuration if using private endpoints
4. Verify connection string format is correct

#### Library Installation Permission Denied (Unity Catalog)

```
PERMISSION_DENIED: 'org.mongodb.spark:mongo-spark-connector_2.12:10.5.0' is not in the artifact allowlist
```

**Solution**: Change the compute access mode:
1. Create a **new compute** (existing compute access mode cannot be changed)
2. Expand **Advanced** section
3. Set **Access mode** to **"Dedicated (formerly: Single user)"**
4. Create the compute, then install the library

#### Class Not Found: MongoTableProvider

```
java.lang.ClassNotFoundException: com.mongodb.spark.sql.connector.MongoTableProvider
```

**Solution**: Install MongoDB Spark Connector:
1. Go to Compute → Your compute → Libraries
2. Install Maven library: `org.mongodb.spark:mongo-spark-connector_2.12:10.5.0`
3. Restart the compute

#### Scala Version Mismatch

```
java.lang.NoSuchMethodError: scala.Predef$.refArrayOps
```

**Solution**: Ensure Scala versions match:
- Databricks Runtime LTS versions use Scala 2.12
- Use connector: `mongo-spark-connector_2.12:10.5.0`
- Do NOT use `_2.13` version

#### Checkpoint Location Error (DBFS Disabled)

```
UnsupportedOperationException: Public DBFS root is disabled. Access is denied on path: /dbfs/checkpoints/...
```

or

```
UnsupportedOperationException: Public DBFS root is disabled. Access is denied on path: /tmp/checkpoints/...
```

**Cause**: Newer Databricks workspaces disable public DBFS access for security.

**Solution**: Use Workspace files path instead. In notebook 03, change the **Checkpoint Path** widget to:
```
/Workspace/Users/<your-email>/checkpoints/odl_streaming
```

Replace `<your-email>` with your Databricks username (e.g., `john.doe@company.com`).

Example:
```
/Workspace/Users/tzehon.tan@outlook.com/checkpoints/odl_streaming
```

### Getting Help

- **Databricks Documentation**: https://docs.databricks.com
- **MongoDB Spark Connector Docs**: https://www.mongodb.com/docs/spark-connector/current/
- **MongoDB Community Forums**: https://www.mongodb.com/community/forums/

### Useful Commands

```python
# Check Spark version
print(f"Spark version: {spark.version}")

# Check Scala version
print(f"Scala version: {spark._jvm.scala.util.Properties.versionString()}")

# List installed libraries
display(dbutils.library.list())

# Check widget values
print(dbutils.widgets.get("mongodb_uri")[:20] + "...")  # Print first 20 chars only

# Check compute configuration
print(spark.sparkContext.getConf().getAll())
```
