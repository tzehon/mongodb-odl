"""Change Streams service for real-time CDC events."""

import asyncio
import logging
from datetime import datetime
from typing import Any, AsyncGenerator, Callable, Dict, List, Optional, Set
from motor.motor_asyncio import AsyncIOMotorCollection

from config import settings
from .mongodb import MongoDBService

logger = logging.getLogger(__name__)


class ChangeStreamService:
    """Service for managing MongoDB Change Streams."""

    def __init__(self, mongodb_service: MongoDBService):
        self.mongodb = mongodb_service
        self._active_streams: Set[str] = set()
        self._callbacks: Dict[str, List[Callable]] = {}

    async def watch_collection(
        self,
        collection_name: str = "account_statements",
        pipeline: Optional[List[dict]] = None,
        account_number: Optional[str] = None,
        consumer_id: Optional[str] = None
    ) -> AsyncGenerator[Dict[str, Any], None]:
        """
        Watch a collection for changes using Change Streams.

        Args:
            collection_name: Name of the collection to watch
            pipeline: Optional aggregation pipeline to filter changes
            account_number: Optional account number to filter changes
            consumer_id: Optional consumer ID for resume token persistence

        Yields:
            Change stream events
        """
        collection = self.mongodb.get_collection(collection_name)

        # Build pipeline
        if pipeline is None:
            pipeline = []

        # Filter by account number if specified
        if account_number:
            pipeline.insert(0, {
                "$match": {
                    "fullDocument.accountNumber": account_number
                }
            })

        # Filter for relevant operations
        pipeline.insert(0, {
            "$match": {
                "operationType": {"$in": ["insert", "update", "replace", "delete"]}
            }
        })

        # Get resume token if consumer_id provided
        resume_token = None
        if consumer_id:
            resume_token = await self.mongodb.get_resume_token(consumer_id)

        # Configure change stream options
        options = {
            "full_document": "updateLookup",
            "batch_size": settings.change_stream_batch_size,
            "max_await_time_ms": settings.change_stream_max_await_time_ms,
        }

        if resume_token:
            options["resume_after"] = resume_token
            logger.info(f"Resuming change stream from token for consumer: {consumer_id}")

        stream_id = f"{collection_name}:{consumer_id or 'anonymous'}"
        self._active_streams.add(stream_id)

        try:
            async with collection.watch(pipeline, **options) as stream:
                logger.info(f"Change stream started for {collection_name}")

                async for change in stream:
                    # Transform the change event
                    event = self._transform_change_event(change)

                    # Save resume token periodically
                    if consumer_id and change.get("_id"):
                        await self.mongodb.save_resume_token(
                            consumer_id,
                            change["_id"]
                        )

                    yield event

        except asyncio.CancelledError:
            logger.info(f"Change stream cancelled for {collection_name}")
            raise
        except Exception as e:
            logger.error(f"Change stream error for {collection_name}: {e}")
            raise
        finally:
            self._active_streams.discard(stream_id)
            logger.info(f"Change stream closed for {collection_name}")

    def _transform_change_event(self, change: Dict) -> Dict[str, Any]:
        """Transform raw change stream event to API format."""
        operation_type = change.get("operationType", "unknown")

        event = {
            "operationType": operation_type,
            "documentKey": change.get("documentKey", {}),
            "timestamp": datetime.utcnow(),
            "clusterTime": change.get("clusterTime"),
        }

        # Include full document for insert/update/replace
        if operation_type in ["insert", "update", "replace"]:
            full_doc = change.get("fullDocument")
            if full_doc:
                # Remove MongoDB internal fields
                full_doc.pop("_id", None)
                event["fullDocument"] = self._summarize_document(full_doc)

        # Include update description for updates
        if operation_type == "update":
            update_desc = change.get("updateDescription", {})
            event["updateDescription"] = {
                "updatedFields": list(update_desc.get("updatedFields", {}).keys()),
                "removedFields": update_desc.get("removedFields", [])
            }

        return event

    def _summarize_document(self, doc: Dict) -> Dict:
        """Create a summary of a document (avoiding full transaction list)."""
        if not doc:
            return {}

        summary = {
            "accountNumber": doc.get("accountNumber"),
            "accountType": doc.get("accountType"),
            "closingBalance": doc.get("closingBalance"),
            "transactionCount": len(doc.get("transactions", [])),
        }

        # Include latest transaction if available
        transactions = doc.get("transactions", [])
        if transactions:
            latest = max(transactions, key=lambda t: t.get("date", datetime.min))
            summary["latestTransaction"] = {
                "transactionId": latest.get("transactionId"),
                "description": latest.get("description"),
                "amount": latest.get("amount"),
                "type": latest.get("type"),
                "date": latest.get("date")
            }

        return summary

    async def get_active_stream_count(self) -> int:
        """Get count of active change streams."""
        return len(self._active_streams)

    async def close_all_streams(self) -> None:
        """Signal to close all active streams."""
        self._active_streams.clear()


class ChangeStreamManager:
    """Manager for multiple change stream consumers."""

    def __init__(self, mongodb_service: MongoDBService):
        self.mongodb = mongodb_service
        self.service = ChangeStreamService(mongodb_service)
        self._consumers: Dict[str, asyncio.Task] = {}

    async def add_consumer(
        self,
        consumer_id: str,
        callback: Callable[[Dict], Any],
        account_number: Optional[str] = None
    ) -> None:
        """Add a change stream consumer with a callback."""
        if consumer_id in self._consumers:
            logger.warning(f"Consumer {consumer_id} already exists")
            return

        async def consumer_task():
            try:
                async for event in self.service.watch_collection(
                    account_number=account_number,
                    consumer_id=consumer_id
                ):
                    await callback(event)
            except asyncio.CancelledError:
                pass
            except Exception as e:
                logger.error(f"Consumer {consumer_id} error: {e}")

        task = asyncio.create_task(consumer_task())
        self._consumers[consumer_id] = task
        logger.info(f"Added change stream consumer: {consumer_id}")

    async def remove_consumer(self, consumer_id: str) -> None:
        """Remove a change stream consumer."""
        if consumer_id not in self._consumers:
            return

        task = self._consumers.pop(consumer_id)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        logger.info(f"Removed change stream consumer: {consumer_id}")

    async def close_all(self) -> None:
        """Close all consumers."""
        for consumer_id in list(self._consumers.keys()):
            await self.remove_consumer(consumer_id)
