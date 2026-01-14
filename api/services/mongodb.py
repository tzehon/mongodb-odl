"""MongoDB service for database operations."""

import logging
import time
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any, Dict, List, Optional
from bson.decimal128 import Decimal128
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase
from pymongo import DESCENDING
from pymongo.errors import ConnectionFailure, OperationFailure

from config import settings

logger = logging.getLogger(__name__)

# Global reference to metrics collector (set during app startup)
_metrics_collector = None
# Global reference to database for explain queries
_db_ref = None
# Benchmark mode: when True, run explain-only (no results needed, just timing)
_benchmark_mode = False
# Global reference to current request (set by middleware)
_current_request = None


def set_metrics_collector(collector):
    """Set the global metrics collector reference."""
    global _metrics_collector
    _metrics_collector = collector


def set_db_ref(db):
    """Set the global database reference for explain queries."""
    global _db_ref
    _db_ref = db


def set_benchmark_mode(enabled: bool):
    """Enable/disable benchmark mode. In benchmark mode, queries run explain-only."""
    global _benchmark_mode
    _benchmark_mode = enabled
    logger.info(f"Benchmark mode: {'enabled' if enabled else 'disabled'}")


def is_benchmark_mode() -> bool:
    """Check if benchmark mode is enabled."""
    return _benchmark_mode


def record_db_execution_time(execution_time_ms: float):
    """Record DB execution time from explain plan."""
    if _metrics_collector and execution_time_ms > 0:
        _metrics_collector.record_db_query(execution_time_ms)
    # Store in request state for response header
    if _current_request is not None:
        _current_request.state.db_exec_time = execution_time_ms


def get_request_db_exec_time() -> float:
    """Get the DB execution time for the current request."""
    if _current_request is not None and hasattr(_current_request.state, 'db_exec_time'):
        return _current_request.state.db_exec_time
    return 0.0


def set_current_request(request):
    """Set the current request reference (called by middleware)."""
    global _current_request
    _current_request = request


def clear_current_request():
    """Clear the current request reference."""
    global _current_request
    _current_request = None


@asynccontextmanager
async def track_db_query():
    """Context manager to track database query execution time (wall clock fallback)."""
    start = time.perf_counter()
    try:
        yield
    finally:
        elapsed_ms = (time.perf_counter() - start) * 1000
        # Only record if no explain-based time was recorded
        # This is a fallback for queries we don't run explain on
        pass  # Disabled - we now use explain-based timing


async def get_explain_execution_time(db, collection_name: str, pipeline: list) -> float:
    """
    Run explain on an aggregation pipeline and return executionTimeMillis.

    This gives the actual MongoDB server execution time, excluding network latency.
    """
    try:
        # Use executionStats verbosity to get actual execution time
        explain_result = await db.command(
            'explain',
            {
                'aggregate': collection_name,
                'pipeline': pipeline,
                'cursor': {}
            },
            verbosity='executionStats'
        )

        # Extract execution time from explain output
        exec_time = 0

        # Standard structure with executionStats
        if 'executionStats' in explain_result:
            exec_time = explain_result['executionStats'].get('executionTimeMillis', 0)

        # Stages structure (aggregation)
        if exec_time == 0 and 'stages' in explain_result:
            for stage in explain_result.get('stages', []):
                if '$cursor' in stage:
                    exec_stats = stage['$cursor'].get('executionStats', {})
                    exec_time = exec_stats.get('executionTimeMillis', 0)
                    break
                # Also check for executionTimeMillisEstimate in stages
                if 'executionTimeMillisEstimate' in stage:
                    exec_time = max(exec_time, stage.get('executionTimeMillisEstimate', 0))

        # Sharded cluster structure
        if exec_time == 0 and 'shards' in explain_result:
            for shard_name, shard_data in explain_result.get('shards', {}).items():
                if 'executionStats' in shard_data:
                    exec_time = max(exec_time, shard_data['executionStats'].get('executionTimeMillis', 0))
                if 'stages' in shard_data:
                    for stage in shard_data['stages']:
                        if '$cursor' in stage:
                            exec_stats = stage['$cursor'].get('executionStats', {})
                            exec_time = max(exec_time, exec_stats.get('executionTimeMillis', 0))

        if exec_time > 0:
            logger.info(f"Explain execution time: {exec_time}ms")
        else:
            logger.warning(f"Explain returned 0ms - explain_result keys: {list(explain_result.keys())}")
        return float(exec_time)
    except Exception as e:
        logger.warning(f"Could not get explain time: {e}")
        return 0.0


async def get_find_explain_time(db, collection_name: str, filter_doc: dict, sort_doc: list = None) -> float:
    """
    Run explain on a find query and return executionTimeMillis.
    """
    try:
        # Build the find command
        cmd = {
            'find': collection_name,
            'filter': filter_doc,
        }
        if sort_doc:
            cmd['sort'] = dict(sort_doc)

        explain_result = await db.command('explain', cmd, verbosity='executionStats')

        exec_time = 0

        # Standard structure with executionStats
        if 'executionStats' in explain_result:
            exec_time = explain_result['executionStats'].get('executionTimeMillis', 0)

        # Stages structure (MongoDB 7.0+ or Atlas)
        if exec_time == 0 and 'stages' in explain_result:
            for stage in explain_result.get('stages', []):
                if '$cursor' in stage:
                    exec_stats = stage['$cursor'].get('executionStats', {})
                    exec_time = exec_stats.get('executionTimeMillis', 0)
                    break
                # Also check for executionTimeMillisEstimate in stages
                if 'executionTimeMillisEstimate' in stage:
                    exec_time = max(exec_time, stage.get('executionTimeMillisEstimate', 0))

        # Sharded structure
        if exec_time == 0 and 'shards' in explain_result:
            for shard_data in explain_result.get('shards', {}).values():
                if 'executionStats' in shard_data:
                    exec_time = max(exec_time, shard_data['executionStats'].get('executionTimeMillis', 0))
                if 'stages' in shard_data:
                    for stage in shard_data['stages']:
                        if '$cursor' in stage:
                            exec_stats = stage['$cursor'].get('executionStats', {})
                            exec_time = max(exec_time, exec_stats.get('executionTimeMillis', 0))

        if exec_time > 0:
            logger.info(f"Find explain execution time: {exec_time}ms")
        else:
            # Log full structure for debugging
            logger.warning(f"Find explain returned 0ms - keys: {list(explain_result.keys())}")
            if 'stages' in explain_result:
                logger.warning(f"Stages structure: {explain_result['stages']}")
        return float(exec_time)
    except Exception as e:
        logger.warning(f"Could not get find explain time: {e}")
        return 0.0


def decimal128_to_float(value) -> float:
    """Convert Decimal128 or other numeric types to float."""
    if isinstance(value, Decimal128):
        return float(value.to_decimal())
    return float(value) if value is not None else 0.0


class MongoDBService:
    """Service for MongoDB database operations."""

    def __init__(self):
        self.client: Optional[AsyncIOMotorClient] = None
        self.db: Optional[AsyncIOMotorDatabase] = None
        self._connected = False

    async def connect(self) -> None:
        """Establish connection to MongoDB."""
        if self._connected:
            return

        logger.info("Connecting to MongoDB...")
        self.client = AsyncIOMotorClient(
            settings.mongodb_uri,
            minPoolSize=settings.mongodb_min_pool_size,
            maxPoolSize=settings.mongodb_max_pool_size,
            maxIdleTimeMS=settings.mongodb_max_idle_time_ms,
            waitQueueTimeoutMS=settings.mongodb_wait_queue_timeout_ms,
            serverSelectionTimeoutMS=settings.mongodb_server_selection_timeout_ms,
        )

        self.db = self.client[settings.mongodb_database]

        # Set global db reference for explain queries
        set_db_ref(self.db)

        # Verify connection
        try:
            await self.client.admin.command("ping")
            self._connected = True
            logger.info(f"Connected to MongoDB database: {settings.mongodb_database}")
        except ConnectionFailure as e:
            logger.error(f"Failed to connect to MongoDB: {e}")
            raise

    async def disconnect(self) -> None:
        """Close MongoDB connection."""
        if self.client:
            self.client.close()
            self._connected = False
            logger.info("Disconnected from MongoDB")

    async def is_healthy(self) -> bool:
        """Check if MongoDB connection is healthy."""
        try:
            await self.client.admin.command("ping")
            return True
        except Exception as e:
            logger.error(f"MongoDB health check failed: {e}")
            return False

    def get_collection(self, name: str):
        """Get a collection by name."""
        return self.db[name]

    # =========================================================================
    # Account Statement Operations
    # =========================================================================

    async def get_latest_statement(self, account_number: str) -> Optional[Dict]:
        """Get the latest statement for an account."""
        collection = self.get_collection("account_statements")
        filter_doc = {"accountNumber": account_number}
        sort_doc = [("statementPeriod.endDate", DESCENDING)]

        # Benchmark mode: run explain-only (actually executes query, returns timing)
        if is_benchmark_mode():
            exec_time = await get_find_explain_time(
                self.db, "account_statements", filter_doc, sort_doc
            )
            record_db_execution_time(exec_time)
            # Return minimal response with exec time for Locust to read
            return {"accountNumber": account_number, "_benchmark": True, "_dbExecTimeMs": exec_time}

        # Normal mode: execute actual query
        statement = await collection.find_one(filter_doc, sort=sort_doc)
        return statement

    async def get_statements_by_date_range(
        self,
        account_number: str,
        start_date: datetime,
        end_date: datetime
    ) -> List[Dict]:
        """Get statements within a date range."""
        collection = self.get_collection("account_statements")
        cursor = collection.find({
            "accountNumber": account_number,
            "statementPeriod.startDate": {"$gte": start_date},
            "statementPeriod.endDate": {"$lte": end_date}
        }).sort("statementPeriod.endDate", DESCENDING)

        return await cursor.to_list(length=100)

    async def get_account_balance(self, account_number: str) -> Optional[Dict]:
        """Get current balance for an account."""
        statement = await self.get_latest_statement(account_number)
        if not statement:
            return None

        # In benchmark mode, pass through the benchmark response with _dbExecTimeMs
        if statement.get("_benchmark"):
            return statement

        return {
            "accountNumber": account_number,
            "currentBalance": decimal128_to_float(statement.get("closingBalance", 0)),
            "currency": statement.get("currency", "SGD"),
            "asOf": statement.get("statementMetadata", {}).get("generatedAt", datetime.now()),
            "accountType": statement.get("accountType", "unknown")
        }

    # =========================================================================
    # Transaction Operations
    # =========================================================================

    async def get_transactions(
        self,
        account_number: str,
        limit: int = 50,
        offset: int = 0,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        min_amount: Optional[float] = None,
        max_amount: Optional[float] = None,
        category: Optional[str] = None,
        tx_type: Optional[str] = None
    ) -> Dict:
        """Get transactions with filtering and pagination."""
        collection = self.get_collection("account_statements")

        # Build aggregation pipeline
        pipeline = [
            {"$match": {"accountNumber": account_number}},
            {"$unwind": "$transactions"},
        ]

        # Apply filters
        match_conditions = {}

        if start_date:
            match_conditions["transactions.date"] = {"$gte": start_date}
        if end_date:
            if "transactions.date" in match_conditions:
                match_conditions["transactions.date"]["$lte"] = end_date
            else:
                match_conditions["transactions.date"] = {"$lte": end_date}

        if min_amount is not None:
            match_conditions["transactions.amount"] = {"$gte": min_amount}
        if max_amount is not None:
            if "transactions.amount" in match_conditions:
                match_conditions["transactions.amount"]["$lte"] = max_amount
            else:
                match_conditions["transactions.amount"] = {"$lte": max_amount}

        if category:
            match_conditions["transactions.category"] = category

        if tx_type:
            match_conditions["transactions.type"] = tx_type

        if match_conditions:
            pipeline.append({"$match": match_conditions})

        # Get paginated results
        pipeline.extend([
            {"$sort": {"transactions.date": -1}},
            {"$skip": offset},
            {"$limit": limit},
            {"$replaceRoot": {"newRoot": "$transactions"}}
        ])

        # Benchmark mode: run explain-only (skip count query - not needed)
        if is_benchmark_mode():
            exec_time = await get_explain_execution_time(
                self.db, "account_statements", pipeline
            )
            record_db_execution_time(exec_time)
            return {
                "items": [],
                "total": 0,
                "page": 1,
                "pageSize": limit,
                "totalPages": 0,
                "hasNext": False,
                "hasPrevious": False,
                "_benchmark": True,
                "_dbExecTimeMs": exec_time
            }

        # Normal mode: run count query + actual query
        count_pipeline = pipeline[:-4] + [{"$count": "total"}]  # Pipeline without sort/skip/limit/replaceRoot
        count_result = await collection.aggregate(count_pipeline).to_list(1)
        total = count_result[0]["total"] if count_result else 0

        transactions = await collection.aggregate(pipeline).to_list(length=limit)

        return {
            "items": transactions,
            "total": total,
            "page": (offset // limit) + 1,
            "pageSize": limit,
            "totalPages": (total + limit - 1) // limit,
            "hasNext": offset + limit < total,
            "hasPrevious": offset > 0
        }

    # =========================================================================
    # Search Operations (Atlas Search)
    # =========================================================================

    async def search_transactions(
        self,
        account_number: str,
        query: str,
        limit: int = 20,
        fuzzy: bool = True
    ) -> List[Dict]:
        """Search transactions using Atlas Search."""
        collection = self.get_collection("account_statements")

        # Build Atlas Search aggregation pipeline
        search_stage = {
            "$search": {
                "index": "transaction_search",
                "compound": {
                    "must": [
                        {
                            "text": {
                                "query": account_number,
                                "path": "accountNumber"
                            }
                        }
                    ],
                    "should": [
                        {
                            "text": {
                                "query": query,
                                "path": "transactions.description",
                                **({"fuzzy": {"maxEdits": 2}} if fuzzy else {}),
                                "score": {"boost": {"value": 2}}
                            }
                        },
                        {
                            "text": {
                                "query": query,
                                "path": "transactions.merchant",
                                **({"fuzzy": {"maxEdits": 1}} if fuzzy else {}),
                                "score": {"boost": {"value": 1.5}}
                            }
                        }
                    ],
                    "minimumShouldMatch": 1
                },
                "highlight": {
                    "path": ["transactions.description", "transactions.merchant"]
                }
            }
        }

        pipeline = [
            search_stage,
            {"$unwind": "$transactions"},
            {
                "$match": {
                    "$or": [
                        {"transactions.description": {"$regex": query, "$options": "i"}},
                        {"transactions.merchant": {"$regex": query, "$options": "i"}}
                    ]
                }
            },
            {
                "$project": {
                    "transactionId": "$transactions.transactionId",
                    "accountNumber": 1,
                    "date": "$transactions.date",
                    "description": "$transactions.description",
                    "amount": "$transactions.amount",
                    "type": "$transactions.type",
                    "category": "$transactions.category",
                    "merchant": "$transactions.merchant",
                    "score": {"$meta": "searchScore"},
                    "highlights": {"$meta": "searchHighlights"}
                }
            },
            {"$sort": {"score": -1}},
            {"$limit": limit}
        ]

        try:
            # Benchmark mode: run explain-only
            if is_benchmark_mode():
                exec_time = await get_explain_execution_time(
                    self.db, "account_statements", pipeline
                )
                record_db_execution_time(exec_time)
                return {"_benchmark": True, "_dbExecTimeMs": exec_time, "items": []}

            # Normal mode: execute actual query
            results = await collection.aggregate(pipeline).to_list(length=limit)
            return results
        except OperationFailure as e:
            # DO NOT silently fallback - make it clear Atlas Search is required
            logger.error(f"Atlas Search FAILED: {e}")
            raise OperationFailure(
                f"Atlas Search index 'transaction_search' not working. Error: {e}"
            )

    async def _fallback_search(
        self,
        account_number: str,
        query: str,
        limit: int
    ) -> List[Dict]:
        """Fallback search using regex when Atlas Search is unavailable."""
        collection = self.get_collection("account_statements")

        pipeline = [
            {"$match": {"accountNumber": account_number}},
            {"$unwind": "$transactions"},
            {
                "$match": {
                    "$or": [
                        {"transactions.description": {"$regex": query, "$options": "i"}},
                        {"transactions.merchant": {"$regex": query, "$options": "i"}}
                    ]
                }
            },
            {
                "$project": {
                    "transactionId": "$transactions.transactionId",
                    "accountNumber": 1,
                    "date": "$transactions.date",
                    "description": "$transactions.description",
                    "amount": "$transactions.amount",
                    "type": "$transactions.type",
                    "category": "$transactions.category",
                    "merchant": "$transactions.merchant",
                    "score": {"$literal": 1.0}
                }
            },
            {"$sort": {"date": -1}},
            {"$limit": limit}
        ]

        # Benchmark mode: run explain-only
        if is_benchmark_mode():
            exec_time = await get_explain_execution_time(
                self.db, "account_statements", pipeline
            )
            record_db_execution_time(exec_time)
            return {"_benchmark": True, "_dbExecTimeMs": exec_time, "items": []}

        return await collection.aggregate(pipeline).to_list(length=limit)

    # =========================================================================
    # Metrics and Stats
    # =========================================================================

    async def get_document_count(self) -> int:
        """Get total document count in account_statements collection."""
        collection = self.get_collection("account_statements")
        return await collection.count_documents({})

    async def get_connection_pool_stats(self) -> Dict:
        """Get connection pool statistics."""
        if not self.client:
            return {}

        # Get server status for connection info
        try:
            server_status = await self.client.admin.command("serverStatus")
            connections = server_status.get("connections", {})
            return {
                "current": connections.get("current", 0),
                "available": connections.get("available", 0),
                "totalCreated": connections.get("totalCreated", 0),
            }
        except Exception as e:
            logger.error(f"Failed to get connection pool stats: {e}")
            return {}

    async def get_all_account_numbers(self, limit: int = 100) -> List[str]:
        """Get list of all account numbers."""
        collection = self.get_collection("account_statements")
        pipeline = [
            {"$group": {"_id": "$accountNumber"}},
            {"$limit": limit},
            {"$project": {"_id": 0, "accountNumber": "$_id"}}
        ]
        results = await collection.aggregate(pipeline).to_list(length=limit)
        return [r["accountNumber"] for r in results]

    # =========================================================================
    # Resume Token Persistence (for Change Streams)
    # =========================================================================

    async def save_resume_token(self, consumer_id: str, token: dict) -> None:
        """Save change stream resume token."""
        collection = self.get_collection("change_stream_resume_tokens")
        await collection.update_one(
            {"_id": consumer_id},
            {
                "$set": {
                    "resumeToken": token,
                    "updatedAt": datetime.utcnow()
                }
            },
            upsert=True
        )

    async def get_resume_token(self, consumer_id: str) -> Optional[dict]:
        """Get saved resume token for a consumer."""
        collection = self.get_collection("change_stream_resume_tokens")
        doc = await collection.find_one({"_id": consumer_id})
        return doc.get("resumeToken") if doc else None


# Global service instance
_mongodb_service: Optional[MongoDBService] = None


async def get_mongodb_service() -> MongoDBService:
    """Get the MongoDB service instance."""
    global _mongodb_service
    if _mongodb_service is None:
        _mongodb_service = MongoDBService()
        await _mongodb_service.connect()
    return _mongodb_service


async def close_mongodb_service() -> None:
    """Close the MongoDB service."""
    global _mongodb_service
    if _mongodb_service:
        await _mongodb_service.disconnect()
        _mongodb_service = None
