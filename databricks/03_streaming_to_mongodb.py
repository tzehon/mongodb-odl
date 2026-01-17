# Databricks notebook source
# MAGIC %md
# MAGIC # Spark Structured Streaming to MongoDB Atlas
# MAGIC
# MAGIC This notebook streams data from Delta Lake to MongoDB Atlas using the MongoDB Spark Connector v10.5.
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - MongoDB Spark Connector v10.5.0 installed as compute library
# MAGIC   - Maven coordinates: `org.mongodb.spark:mongo-spark-connector_2.12:10.5.0`
# MAGIC - MongoDB Atlas connection string (enter in the widget when running)
# MAGIC - Delta tables created (run 01_setup_delta_tables.py first)
# MAGIC - Data generated (run 02_data_generator.py first)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration Widgets

# COMMAND ----------

dbutils.widgets.text("mongodb_uri", "", "MongoDB Atlas URI")
dbutils.widgets.text("catalog", "main", "Catalog Name")
dbutils.widgets.text("schema", "banking_odl", "Schema/Database Name")
dbutils.widgets.text("mongodb_database", "banking_odl", "MongoDB Database")
dbutils.widgets.text("mongodb_collection", "account_statements", "MongoDB Collection")
# Note: Update this path with your username if DBFS is disabled
# Use format: /Workspace/Users/<your-email>/checkpoints/odl_streaming
dbutils.widgets.text("checkpoint_path", "/Workspace/checkpoints/odl_streaming/account_statements", "Checkpoint Path")
dbutils.widgets.dropdown("start_fresh", "true", ["true", "false"], "Start Fresh (clear checkpoint)")

# COMMAND ----------

# Get widget values
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
mongodb_database = dbutils.widgets.get("mongodb_database")
mongodb_collection = dbutils.widgets.get("mongodb_collection")
checkpoint_path = dbutils.widgets.get("checkpoint_path")
start_fresh = dbutils.widgets.get("start_fresh") == "true"

print(f"Configuration:")
print(f"  Source: {catalog}.{schema}.account_statements")
print(f"  Target: MongoDB {mongodb_database}.{mongodb_collection}")
print(f"  Checkpoint Path: {checkpoint_path}")
print(f"  Start Fresh: {start_fresh}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup MongoDB Connection

# COMMAND ----------

# Get MongoDB URI from widget
mongodb_uri = dbutils.widgets.get("mongodb_uri")

if not mongodb_uri or mongodb_uri.strip() == "":
    raise ValueError(
        "MongoDB URI is required!\n\n"
        "Please enter your MongoDB Atlas connection string in the 'MongoDB Atlas URI' widget at the top of the notebook.\n"
        "Format: mongodb+srv://<username>:<password>@<cluster>.mongodb.net/<database>?retryWrites=true&w=majority"
    )

print("MongoDB URI configured successfully")
print(f"URI starts with: {mongodb_uri[:30]}...")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clear Checkpoint (if starting fresh)

# COMMAND ----------

if start_fresh:
    print(f"Clearing checkpoint at {checkpoint_path}...")
    try:
        dbutils.fs.rm(checkpoint_path, recurse=True)
        print("Checkpoint cleared successfully")
    except Exception as e:
        print(f"Could not clear checkpoint (may not exist): {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Initialize Spark Session

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, struct, current_timestamp, expr
from pyspark.sql.types import *

# Get or create Spark session
spark = SparkSession.builder \
    .appName("ODL-Streaming-MongoDB") \
    .config("spark.sql.shuffle.partitions", "8") \
    .config("spark.streaming.stopGracefullyOnShutdown", "true") \
    .getOrCreate()

print(f"Spark version: {spark.version}")

# COMMAND ----------

# Set catalog and schema
try:
    spark.sql(f"USE CATALOG {catalog}")
    print(f"Using catalog: {catalog}")
except Exception as e:
    print(f"Could not use catalog '{catalog}' (may not be using Unity Catalog): {e}")
    print("Continuing with default catalog...")

spark.sql(f"USE SCHEMA {schema}")
print(f"Using schema: {schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Stream from Delta Lake

# COMMAND ----------

# Read from Delta table as a stream
# Using Change Data Feed to capture only changes
delta_stream = spark.readStream \
    .format("delta") \
    .option("readChangeFeed", "true") \
    .option("startingVersion", "0") \
    .table("account_statements")

print("Delta stream initialized")
print(f"Schema: {delta_stream.schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transform Data for MongoDB
# MAGIC
# MAGIC The MongoDB Spark Connector expects data in a specific format.
# MAGIC We'll transform column names from snake_case to camelCase to match the MongoDB schema.

# COMMAND ----------

def transform_for_mongodb(df):
    """Transform DataFrame columns for MongoDB schema compatibility."""

    # Filter for inserts and updates only (from Change Data Feed)
    # _change_type can be: insert, update_preimage, update_postimage, delete
    filtered = df.filter(
        col("_change_type").isin(["insert", "update_postimage"])
    )

    # Transform nested structures to match MongoDB schema
    transformed = filtered \
        .withColumn("accountNumber", col("account_number")) \
        .withColumn("accountHolder", struct(
            col("account_holder.name").alias("name"),
            col("account_holder.email").alias("email"),
            col("account_holder.phone").alias("phone"),
            struct(
                col("account_holder.address.line1").alias("line1"),
                col("account_holder.address.line2").alias("line2"),
                col("account_holder.address.city").alias("city"),
                col("account_holder.address.postal_code").alias("postalCode"),
                col("account_holder.address.country").alias("country")
            ).alias("address")
        )) \
        .withColumn("accountType", col("account_type")) \
        .withColumn("branch", struct(
            col("branch.code").alias("code"),
            col("branch.name").alias("name"),
            col("branch.region").alias("region")
        )) \
        .withColumn("statementPeriod", struct(
            col("statement_period.start_date").alias("startDate"),
            col("statement_period.end_date").alias("endDate")
        )) \
        .withColumn("openingBalance", col("opening_balance")) \
        .withColumn("closingBalance", col("closing_balance")) \
        .withColumn("statementMetadata", struct(
            col("statement_metadata.generated_at").alias("generatedAt"),
            col("statement_metadata.version").alias("version"),
            col("statement_metadata.source").alias("source")
        ))

    # Transform the transactions array to use camelCase
    with_transactions = transformed.withColumn(
        "transactions",
        expr("""
            transform(transactions, t -> struct(
                t.transaction_id as transactionId,
                t.date as date,
                t.description as description,
                t.amount as amount,
                t.type as type,
                t.category as category,
                t.merchant as merchant,
                t.reference_number as referenceNumber,
                t.running_balance as runningBalance,
                struct(
                    t.metadata.channel as channel,
                    t.metadata.location as location
                ) as metadata
            ))
        """)
    )

    # Add sync timestamp - when data is written to MongoDB
    # This is used to calculate end-to-end latency: _syncedAt - statementMetadata.generatedAt
    with_sync_time = with_transactions.withColumn(
        "_syncedAt", current_timestamp()
    )

    # Select only the MongoDB-formatted columns
    result = with_sync_time.select(
        "accountNumber",
        "accountHolder",
        "accountType",
        "branch",
        "currency",
        "statementPeriod",
        "openingBalance",
        "closingBalance",
        "transactions",
        "statementMetadata",
        "_syncedAt",
        "_created_at",
        "_updated_at"
    )

    return result

# COMMAND ----------

# MAGIC %md
# MAGIC ## Start the Streaming Query
# MAGIC
# MAGIC Using native MongoDB Spark Connector streaming mode for optimal performance.
# MAGIC Data is processed as soon as it arrives (no artificial delays).

# COMMAND ----------

# Transform the stream
transformed_stream = transform_for_mongodb(delta_stream)

# Start the streaming query using native MongoDB sink
query = transformed_stream \
    .writeStream \
    .format("mongodb") \
    .option("spark.mongodb.connection.uri", mongodb_uri) \
    .option("spark.mongodb.database", mongodb_database) \
    .option("spark.mongodb.collection", mongodb_collection) \
    .option("spark.mongodb.operationType", "update") \
    .option("spark.mongodb.upsertDocument", "true") \
    .option("spark.mongodb.idFieldList", "accountNumber,statementPeriod.startDate") \
    .option("checkpointLocation", checkpoint_path) \
    .queryName("odl_mongodb_streaming") \
    .start()

print(f"Streaming query started: {query.name}")
print(f"Query ID: {query.id}")
print(f"Status: {query.status}")
print(f"\nStreaming mode: Native MongoDB sink (processes data as it arrives)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Monitor the Stream

# COMMAND ----------

# Display streaming query status
import time

# Monitor for a few iterations
for i in range(5):
    status = query.status
    progress = query.lastProgress

    print(f"\n{'='*60}")
    print(f"Iteration {i+1}")
    print(f"{'='*60}")
    print(f"Is Active: {query.isActive}")
    print(f"Status: {status}")

    if progress:
        print(f"\nLast Progress:")
        print(f"  Input Rows: {progress.get('numInputRows', 'N/A')}")
        print(f"  Processing Time: {progress.get('batchDuration', 'N/A')} ms")
        print(f"  Input Rate: {progress.get('inputRowsPerSecond', 'N/A')} rows/sec")
        print(f"  Process Rate: {progress.get('processedRowsPerSecond', 'N/A')} rows/sec")

    time.sleep(15)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stream Management Commands
# MAGIC
# MAGIC Use these commands to manage the streaming query:

# COMMAND ----------

# To stop the stream gracefully, uncomment:
# query.stop()

# To check if stream is still running:
print(f"Stream is active: {query.isActive}")

# To see all active streams:
for q in spark.streams.active:
    print(f"Active stream: {q.name} (ID: {q.id})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Useful Monitoring Queries

# COMMAND ----------

# Check MongoDB collection stats (requires running this in a separate cell after some data is written)
# Note: This requires pymongo which may not be installed by default

# from pymongo import MongoClient
# client = MongoClient(mongodb_uri)
# db = client[mongodb_database]
# collection = db[mongodb_collection]
# print(f"Document count in MongoDB: {collection.count_documents({})}")
# print(f"Collection stats: {db.command('collStats', mongodb_collection)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Streaming Query Await Termination
# MAGIC
# MAGIC Run this cell to keep the notebook running and streaming:

# COMMAND ----------

# Await termination - this will block until the stream is stopped
# Uncomment to keep streaming:
# query.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup and Restart Instructions
# MAGIC
# MAGIC ### Default behavior (`start_fresh=true`):
# MAGIC - Clears checkpoint and reprocesses all data from Delta Lake
# MAGIC - Use this for fresh runs or after resetting Delta tables
# MAGIC
# MAGIC ### To resume streaming from checkpoint:
# MAGIC 1. Set `start_fresh` widget to `false`
# MAGIC 2. Re-run all cells
# MAGIC 3. The stream will resume from the last checkpoint (only processes new changes)
# MAGIC
# MAGIC ### To stop streaming:
# MAGIC ```python
# MAGIC query.stop()
# MAGIC ```
# MAGIC
# MAGIC ### When to use each mode:
# MAGIC | Scenario | `start_fresh` setting |
# MAGIC |----------|----------------------|
# MAGIC | First run | `true` (default) |
# MAGIC | After resetting Delta tables | `true` (default) |
# MAGIC | Resume after compute restart (want new data only) | `false` |
