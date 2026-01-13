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
dbutils.widgets.text("trigger_interval", "10 seconds", "Trigger Interval")
# Note: Update this path with your username if DBFS is disabled
# Use format: /Workspace/Users/<your-email>/checkpoints/odl_streaming
dbutils.widgets.text("checkpoint_path", "/Workspace/checkpoints/odl_streaming/account_statements", "Checkpoint Path")
dbutils.widgets.dropdown("start_fresh", "false", ["true", "false"], "Start Fresh (clear checkpoint)")

# COMMAND ----------

# Get widget values
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
mongodb_database = dbutils.widgets.get("mongodb_database")
mongodb_collection = dbutils.widgets.get("mongodb_collection")
trigger_interval = dbutils.widgets.get("trigger_interval")
checkpoint_path = dbutils.widgets.get("checkpoint_path")
start_fresh = dbutils.widgets.get("start_fresh") == "true"

print(f"Configuration:")
print(f"  Source: {catalog}.{schema}.account_statements")
print(f"  Target: MongoDB {mongodb_database}.{mongodb_collection}")
print(f"  Trigger Interval: {trigger_interval}")
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
from pyspark.sql.functions import col, to_json, struct, current_timestamp, lit
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

from pyspark.sql.functions import col, struct, array, when

def transform_for_mongodb(df):
    """Transform DataFrame columns for MongoDB schema compatibility."""

    # Transform nested structures to match MongoDB schema
    transformed = df \
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

    # Select only the MongoDB-formatted columns
    result = transformed.select(
        "accountNumber",
        "accountHolder",
        "accountType",
        "branch",
        "currency",
        "statementPeriod",
        "openingBalance",
        "closingBalance",
        "transactions",  # Keep transactions array as-is for now
        "statementMetadata",
        "_created_at",
        "_updated_at"
    )

    return result

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Stream to MongoDB using foreachBatch
# MAGIC
# MAGIC Using foreachBatch allows us to:
# MAGIC - Handle upserts (update existing documents or insert new ones)
# MAGIC - Transform data within each micro-batch
# MAGIC - Handle errors gracefully

# COMMAND ----------

from pyspark.sql.functions import expr, transform, struct, col

def write_to_mongodb(batch_df, batch_id):
    """Write a micro-batch to MongoDB with upsert logic."""
    if batch_df.isEmpty():
        print(f"Batch {batch_id}: Empty batch, skipping")
        return

    record_count = batch_df.count()
    print(f"Batch {batch_id}: Processing {record_count} records...")

    try:
        # Filter for inserts and updates (from Change Data Feed)
        # _change_type can be: insert, update_preimage, update_postimage, delete
        actionable_df = batch_df.filter(
            col("_change_type").isin(["insert", "update_postimage"])
        )

        if actionable_df.isEmpty():
            print(f"Batch {batch_id}: No actionable changes (inserts/updates)")
            return

        # Transform the data for MongoDB schema
        transformed_df = transform_for_mongodb(actionable_df)

        # Transform the transactions array to use camelCase
        # This requires a more complex transformation
        final_df = transformed_df.withColumn(
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

        # Write to MongoDB using the Spark Connector
        final_df.write \
            .format("mongodb") \
            .option("connection.uri", mongodb_uri) \
            .option("database", mongodb_database) \
            .option("collection", mongodb_collection) \
            .option("operationType", "update") \
            .option("upsertDocument", "true") \
            .option("idFieldList", "accountNumber,statementPeriod.startDate") \
            .mode("append") \
            .save()

        print(f"Batch {batch_id}: Successfully wrote {actionable_df.count()} records to MongoDB")

    except Exception as e:
        print(f"Batch {batch_id}: Error writing to MongoDB: {e}")
        # In production, you might want to write to a dead letter queue
        # For now, we'll re-raise to fail the batch
        raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Start the Streaming Query

# COMMAND ----------

# Start the streaming query
query = delta_stream \
    .writeStream \
    .foreachBatch(write_to_mongodb) \
    .option("checkpointLocation", checkpoint_path) \
    .trigger(processingTime=trigger_interval) \
    .queryName("odl_mongodb_streaming") \
    .start()

print(f"Streaming query started: {query.name}")
print(f"Query ID: {query.id}")
print(f"Status: {query.status}")

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
# MAGIC ## Error Handling and Dead Letter Queue
# MAGIC
# MAGIC For production deployments, implement a dead letter queue pattern:

# COMMAND ----------

# Dead Letter Queue implementation (for production use)
def write_to_mongodb_with_dlq(batch_df, batch_id):
    """Write to MongoDB with dead letter queue for failed records."""
    if batch_df.isEmpty():
        return

    # Filter actionable changes
    actionable_df = batch_df.filter(
        col("_change_type").isin(["insert", "update_postimage"])
    )

    if actionable_df.isEmpty():
        return

    try:
        # Transform and write
        transformed_df = transform_for_mongodb(actionable_df)

        final_df = transformed_df.withColumn(
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

        final_df.write \
            .format("mongodb") \
            .option("connection.uri", mongodb_uri) \
            .option("database", mongodb_database) \
            .option("collection", mongodb_collection) \
            .option("operationType", "update") \
            .option("upsertDocument", "true") \
            .option("idFieldList", "accountNumber,statementPeriod.startDate") \
            .mode("append") \
            .save()

    except Exception as e:
        print(f"Error in batch {batch_id}, writing to DLQ: {e}")

        # Write failed records to dead letter queue (Delta table)
        dlq_df = actionable_df \
            .withColumn("_error_message", lit(str(e))) \
            .withColumn("_error_timestamp", current_timestamp()) \
            .withColumn("_batch_id", lit(batch_id))

        dlq_df.write \
            .format("delta") \
            .mode("append") \
            .saveAsTable("account_statements_dlq")

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
# MAGIC ### To restart streaming from scratch:
# MAGIC 1. Set `start_fresh` widget to `true`
# MAGIC 2. Re-run all cells
# MAGIC
# MAGIC ### To stop streaming:
# MAGIC ```python
# MAGIC query.stop()
# MAGIC ```
# MAGIC
# MAGIC ### To resume streaming after notebook restart:
# MAGIC 1. Keep `start_fresh` as `false`
# MAGIC 2. The stream will resume from the last checkpoint
