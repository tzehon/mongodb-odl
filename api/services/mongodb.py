"""MongoDB service for database operations."""

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional
from bson.decimal128 import Decimal128
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase
from pymongo import DESCENDING
from pymongo.errors import ConnectionFailure, OperationFailure

from config import settings

logger = logging.getLogger(__name__)


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
        statement = await collection.find_one(
            {"accountNumber": account_number},
            sort=[("statementPeriod.endDate", DESCENDING)]
        )
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

        # Count total matching transactions
        count_pipeline = pipeline + [{"$count": "total"}]
        count_result = await collection.aggregate(count_pipeline).to_list(1)
        total = count_result[0]["total"] if count_result else 0

        # Get paginated results
        pipeline.extend([
            {"$sort": {"transactions.date": -1}},
            {"$skip": offset},
            {"$limit": limit},
            {"$replaceRoot": {"newRoot": "$transactions"}}
        ])

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
                                "fuzzy": {"maxEdits": 2} if fuzzy else {},
                                "score": {"boost": {"value": 2}}
                            }
                        },
                        {
                            "text": {
                                "query": query,
                                "path": "transactions.merchant",
                                "fuzzy": {"maxEdits": 1} if fuzzy else {},
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
            results = await collection.aggregate(pipeline).to_list(length=limit)
            return results
        except OperationFailure as e:
            # Fallback to regex search if Atlas Search is not available
            logger.warning(f"Atlas Search failed, falling back to regex: {e}")
            return await self._fallback_search(account_number, query, limit)

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
