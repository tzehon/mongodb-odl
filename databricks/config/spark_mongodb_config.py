# Databricks notebook source
# MAGIC %md
# MAGIC # MongoDB Spark Configuration Module
# MAGIC
# MAGIC This module provides configuration utilities for connecting Spark to MongoDB Atlas.
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - MongoDB Spark Connector v10.5.0 installed as cluster library
# MAGIC - Databricks Secrets scope 'mongodb' with 'atlas_uri' key configured

# COMMAND ----------

from pyspark.sql import SparkSession
from dataclasses import dataclass
from typing import Optional
import os

# COMMAND ----------

@dataclass
class MongoDBConfig:
    """Configuration for MongoDB connection."""
    uri: str
    database: str
    collection: str

    # Write options
    write_operation_type: str = "update"
    upsert_document: bool = True
    batch_size: int = 512

    # Read options
    read_preference: str = "secondaryPreferred"

    @classmethod
    def from_secrets(cls, database: str, collection: str, scope: str = "mongodb", key: str = "atlas_uri"):
        """Create config from Databricks secrets."""
        try:
            uri = dbutils.secrets.get(scope=scope, key=key)
        except Exception as e:
            raise ValueError(f"Failed to retrieve MongoDB URI from secrets scope '{scope}', key '{key}': {e}")

        return cls(uri=uri, database=database, collection=collection)

    @classmethod
    def from_env(cls, database: str, collection: str):
        """Create config from environment variable (for local testing)."""
        uri = os.environ.get("MONGODB_URI")
        if not uri:
            raise ValueError("MONGODB_URI environment variable not set")
        return cls(uri=uri, database=database, collection=collection)


# COMMAND ----------

@dataclass
class EnvironmentConfig:
    """Environment-specific configuration."""
    name: str
    database_suffix: str
    checkpoint_base: str

    @property
    def checkpoint_path(self) -> str:
        return f"{self.checkpoint_base}/{self.name}"


# Predefined environments
ENVIRONMENTS = {
    "dev": EnvironmentConfig(
        name="dev",
        database_suffix="_dev",
        checkpoint_base="/dbfs/checkpoints/odl_streaming"
    ),
    "staging": EnvironmentConfig(
        name="staging",
        database_suffix="_staging",
        checkpoint_base="/dbfs/checkpoints/odl_streaming"
    ),
    "prod": EnvironmentConfig(
        name="prod",
        database_suffix="",
        checkpoint_base="/dbfs/checkpoints/odl_streaming"
    )
}


# COMMAND ----------

def get_spark_session_with_mongodb(app_name: str = "ODL-Streaming") -> SparkSession:
    """
    Get or create a Spark session configured for MongoDB.

    Note: MongoDB Spark Connector must be installed as a cluster library:
    Maven coordinates: org.mongodb.spark:mongo-spark-connector_2.12:10.5.0
    """
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.shuffle.partitions", "8") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .getOrCreate()

    return spark


# COMMAND ----------

def get_mongodb_write_options(config: MongoDBConfig) -> dict:
    """Get write options for MongoDB Spark Connector."""
    return {
        "spark.mongodb.connection.uri": config.uri,
        "spark.mongodb.database": config.database,
        "spark.mongodb.collection": config.collection,
        "spark.mongodb.write.operationType": config.write_operation_type,
        "spark.mongodb.write.upsertDocument": str(config.upsert_document).lower(),
        "spark.mongodb.output.batchSize": str(config.batch_size),
    }


def get_mongodb_read_options(config: MongoDBConfig) -> dict:
    """Get read options for MongoDB Spark Connector."""
    return {
        "spark.mongodb.connection.uri": config.uri,
        "spark.mongodb.database": config.database,
        "spark.mongodb.collection": config.collection,
        "spark.mongodb.read.readPreference.name": config.read_preference,
    }


# COMMAND ----------

# MAGIC %md
# MAGIC ## Usage Example
# MAGIC
# MAGIC ```python
# MAGIC from config.spark_mongodb_config import MongoDBConfig, get_spark_session_with_mongodb, get_mongodb_write_options
# MAGIC
# MAGIC # Initialize config from Databricks secrets
# MAGIC config = MongoDBConfig.from_secrets(
# MAGIC     database="banking_odl",
# MAGIC     collection="account_statements"
# MAGIC )
# MAGIC
# MAGIC # Get Spark session
# MAGIC spark = get_spark_session_with_mongodb()
# MAGIC
# MAGIC # Get write options for streaming
# MAGIC write_options = get_mongodb_write_options(config)
# MAGIC ```
