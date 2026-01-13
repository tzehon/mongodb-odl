# Databricks notebook source
# MAGIC %md
# MAGIC # Delta Lake Setup for Banking ODL Demo
# MAGIC
# MAGIC This notebook creates the Delta Lake tables for storing banking account statements.
# MAGIC These tables serve as the source for streaming to MongoDB Atlas ODL.
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - Databricks Runtime 13.3 LTS or later
# MAGIC - Unity Catalog enabled (optional but recommended)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration Widgets

# COMMAND ----------

# Create widgets for configuration
dbutils.widgets.text("catalog", "main", "Catalog Name")
dbutils.widgets.text("schema", "banking_odl", "Schema/Database Name")
dbutils.widgets.dropdown("reset_tables", "false", ["true", "false"], "Reset Tables")

# COMMAND ----------

# Get widget values
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
reset_tables = dbutils.widgets.get("reset_tables") == "true"

print(f"Catalog: {catalog}")
print(f"Schema: {schema}")
print(f"Reset Tables: {reset_tables}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Catalog and Schema

# COMMAND ----------

# Create catalog if using Unity Catalog
try:
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
    spark.sql(f"USE CATALOG {catalog}")
    print(f"Using catalog: {catalog}")
except Exception as e:
    print(f"Could not create/use catalog (may not be using Unity Catalog): {e}")
    print("Continuing with default catalog...")

# Create schema/database
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
spark.sql(f"USE SCHEMA {schema}")
print(f"Using schema: {schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reset Tables (if requested)

# COMMAND ----------

if reset_tables:
    print("Resetting tables...")
    spark.sql("DROP TABLE IF EXISTS account_statements")
    spark.sql("DROP TABLE IF EXISTS transactions_stream")
    print("Tables dropped.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Account Statements Table
# MAGIC
# MAGIC This table stores the full account statement documents that will be synced to MongoDB.

# COMMAND ----------

# Create the account_statements Delta table
spark.sql("""
CREATE TABLE IF NOT EXISTS account_statements (
    -- Primary identifiers
    account_number STRING NOT NULL,

    -- Account holder information
    account_holder STRUCT<
        name: STRING,
        email: STRING,
        phone: STRING,
        address: STRUCT<
            line1: STRING,
            line2: STRING,
            city: STRING,
            postal_code: STRING,
            country: STRING
        >
    >,

    -- Account details
    account_type STRING,
    branch STRUCT<
        code: STRING,
        name: STRING,
        region: STRING
    >,
    currency STRING,

    -- Statement period
    statement_period STRUCT<
        start_date: TIMESTAMP,
        end_date: TIMESTAMP
    >,

    -- Balances
    opening_balance DECIMAL(18, 2),
    closing_balance DECIMAL(18, 2),

    -- Transactions array
    transactions ARRAY<STRUCT<
        transaction_id: STRING,
        date: TIMESTAMP,
        description: STRING,
        amount: DECIMAL(18, 2),
        type: STRING,
        category: STRING,
        merchant: STRING,
        reference_number: STRING,
        running_balance: DECIMAL(18, 2),
        metadata: STRUCT<
            channel: STRING,
            location: STRING
        >
    >>,

    -- Metadata
    statement_metadata STRUCT<
        generated_at: TIMESTAMP,
        version: INT,
        source: STRING
    >,

    -- Processing metadata
    _created_at TIMESTAMP,
    _updated_at TIMESTAMP
)
USING DELTA
PARTITIONED BY (account_type)
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.columnMapping.mode' = 'name'
)
COMMENT 'Banking account statements for ODL demo - synced to MongoDB Atlas'
""")

print("Created account_statements table")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Transactions Stream Table
# MAGIC
# MAGIC This table captures individual transactions as they arrive (before aggregation into statements).
# MAGIC This is useful for real-time transaction streaming scenarios.

# COMMAND ----------

# Create the transactions_stream Delta table
spark.sql("""
CREATE TABLE IF NOT EXISTS transactions_stream (
    transaction_id STRING NOT NULL,
    account_number STRING NOT NULL,
    date TIMESTAMP,
    description STRING,
    amount DECIMAL(18, 2),
    type STRING,
    category STRING,
    merchant STRING,
    reference_number STRING,
    channel STRING,
    location STRING,
    _ingested_at TIMESTAMP,
    date_partition STRING GENERATED ALWAYS AS (DATE_FORMAT(date, 'yyyy-MM'))
)
USING DELTA
PARTITIONED BY (date_partition)
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.columnMapping.mode' = 'name'
)
COMMENT 'Individual banking transactions stream for ODL demo'
""")

print("Created transactions_stream table")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Indexes using Z-ORDER
# MAGIC
# MAGIC Delta Lake uses Z-ORDER for data skipping optimization instead of traditional indexes.

# COMMAND ----------

# Optimize and Z-ORDER the tables for common query patterns
# This is typically run periodically, not just at setup
# Uncommenting these for initial setup:

# spark.sql("""
# OPTIMIZE account_statements
# ZORDER BY (account_number, statement_period.end_date)
# """)
# print("Optimized account_statements with ZORDER")

# spark.sql("""
# OPTIMIZE transactions_stream
# ZORDER BY (account_number, date)
# """)
# print("Optimized transactions_stream with ZORDER")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Table Creation

# COMMAND ----------

# Display table information
print("=" * 60)
print("ACCOUNT_STATEMENTS TABLE")
print("=" * 60)
display(spark.sql("DESCRIBE EXTENDED account_statements"))

# COMMAND ----------

print("=" * 60)
print("TRANSACTIONS_STREAM TABLE")
print("=" * 60)
display(spark.sql("DESCRIBE EXTENDED transactions_stream"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Table Properties Summary

# COMMAND ----------

# Check that Change Data Feed is enabled
tables = ["account_statements", "transactions_stream"]

for table in tables:
    props = spark.sql(f"SHOW TBLPROPERTIES {table}").collect()
    cdf_enabled = any(row.key == "delta.enableChangeDataFeed" and row.value == "true" for row in props)
    print(f"{table}: Change Data Feed enabled = {cdf_enabled}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC
# MAGIC 1. Run `02_data_generator.py` to generate sample banking data
# MAGIC 2. Run `03_streaming_to_mongodb.py` to start streaming to MongoDB Atlas
# MAGIC
# MAGIC ## Useful Commands
# MAGIC
# MAGIC ```sql
# MAGIC -- View recent changes (CDC)
# MAGIC SELECT * FROM table_changes('account_statements', 1) LIMIT 10;
# MAGIC
# MAGIC -- View table history
# MAGIC DESCRIBE HISTORY account_statements;
# MAGIC
# MAGIC -- Manually optimize (run periodically)
# MAGIC OPTIMIZE account_statements ZORDER BY (account_number, statement_period.end_date);
# MAGIC ```
