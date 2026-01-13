"""WebSocket endpoint for real-time change stream events."""

import asyncio
import json
import logging
from datetime import datetime
from typing import Dict, Optional, Set
from fastapi import APIRouter, WebSocket, WebSocketDisconnect
import uuid

from services import ChangeStreamService, get_mongodb_service

logger = logging.getLogger(__name__)
router = APIRouter()


class ConnectionManager:
    """Manages WebSocket connections for change stream events."""

    def __init__(self):
        self.active_connections: Dict[str, WebSocket] = {}
        self.connection_filters: Dict[str, Optional[str]] = {}  # connection_id -> account_number filter

    async def connect(self, websocket: WebSocket, account_number: Optional[str] = None) -> str:
        """Accept a new WebSocket connection."""
        await websocket.accept()
        connection_id = str(uuid.uuid4())
        self.active_connections[connection_id] = websocket
        self.connection_filters[connection_id] = account_number
        logger.info(f"WebSocket connected: {connection_id} (filter: {account_number})")
        return connection_id

    def disconnect(self, connection_id: str):
        """Remove a WebSocket connection."""
        self.active_connections.pop(connection_id, None)
        self.connection_filters.pop(connection_id, None)
        logger.info(f"WebSocket disconnected: {connection_id}")

    async def send_event(self, connection_id: str, event: dict):
        """Send an event to a specific connection."""
        websocket = self.active_connections.get(connection_id)
        if websocket:
            try:
                await websocket.send_json(event)
            except Exception as e:
                logger.error(f"Failed to send to {connection_id}: {e}")
                self.disconnect(connection_id)

    async def broadcast(self, event: dict, account_number: Optional[str] = None):
        """Broadcast an event to all connections (or filtered by account)."""
        disconnected = []

        for conn_id, websocket in self.active_connections.items():
            # Check if this connection should receive the event
            conn_filter = self.connection_filters.get(conn_id)

            # Send if: no filter on connection, no filter on event, or filters match
            if conn_filter is None or account_number is None or conn_filter == account_number:
                try:
                    await websocket.send_json(event)
                except Exception as e:
                    logger.error(f"Failed to broadcast to {conn_id}: {e}")
                    disconnected.append(conn_id)

        # Clean up disconnected connections
        for conn_id in disconnected:
            self.disconnect(conn_id)

    @property
    def connection_count(self) -> int:
        """Get the number of active connections."""
        return len(self.active_connections)


# Global connection manager
manager = ConnectionManager()


def serialize_datetime(obj):
    """JSON serializer for datetime objects."""
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")


@router.websocket("/ws/changes")
async def websocket_changes(websocket: WebSocket):
    """
    WebSocket endpoint for all change stream events.

    Connects to MongoDB Change Streams and broadcasts events to all connected clients.

    Message format (outgoing):
    {
        "operationType": "insert|update|replace|delete",
        "documentKey": {...},
        "timestamp": "ISO8601 timestamp",
        "fullDocument": {...},  // for insert/update/replace
        "updateDescription": {...}  // for updates
    }
    """
    connection_id = await manager.connect(websocket)

    try:
        # Send connection confirmation
        await websocket.send_json({
            "type": "connected",
            "connectionId": connection_id,
            "message": "Connected to change stream",
            "timestamp": datetime.utcnow().isoformat()
        })

        # Start listening for changes
        mongodb = await get_mongodb_service()
        change_service = ChangeStreamService(mongodb)

        async for event in change_service.watch_collection(
            consumer_id=f"ws_{connection_id}"
        ):
            # Broadcast to all connections
            await manager.broadcast(event)

    except WebSocketDisconnect:
        logger.info(f"WebSocket {connection_id} disconnected by client")
    except asyncio.CancelledError:
        logger.info(f"WebSocket {connection_id} cancelled")
    except Exception as e:
        logger.error(f"WebSocket {connection_id} error: {e}")
        try:
            await websocket.send_json({
                "type": "error",
                "message": str(e),
                "timestamp": datetime.utcnow().isoformat()
            })
        except Exception:
            pass
    finally:
        manager.disconnect(connection_id)


@router.websocket("/ws/changes/{account_number}")
async def websocket_changes_filtered(websocket: WebSocket, account_number: str):
    """
    WebSocket endpoint for change stream events filtered by account number.

    Only sends events for the specified account.
    """
    connection_id = await manager.connect(websocket, account_number=account_number)

    try:
        # Send connection confirmation
        await websocket.send_json({
            "type": "connected",
            "connectionId": connection_id,
            "accountNumber": account_number,
            "message": f"Connected to change stream for account {account_number}",
            "timestamp": datetime.utcnow().isoformat()
        })

        # Start listening for changes
        mongodb = await get_mongodb_service()
        change_service = ChangeStreamService(mongodb)

        async for event in change_service.watch_collection(
            account_number=account_number,
            consumer_id=f"ws_{connection_id}_{account_number}"
        ):
            # Send only to this connection (already filtered by account)
            await manager.send_event(connection_id, event)

    except WebSocketDisconnect:
        logger.info(f"WebSocket {connection_id} disconnected by client")
    except asyncio.CancelledError:
        logger.info(f"WebSocket {connection_id} cancelled")
    except Exception as e:
        logger.error(f"WebSocket {connection_id} error: {e}")
        try:
            await websocket.send_json({
                "type": "error",
                "message": str(e),
                "timestamp": datetime.utcnow().isoformat()
            })
        except Exception:
            pass
    finally:
        manager.disconnect(connection_id)


@router.get("/ws/status")
async def websocket_status():
    """Get WebSocket connection status."""
    return {
        "activeConnections": manager.connection_count,
        "connections": [
            {
                "connectionId": conn_id,
                "filter": manager.connection_filters.get(conn_id)
            }
            for conn_id in manager.active_connections.keys()
        ],
        "timestamp": datetime.utcnow().isoformat()
    }
