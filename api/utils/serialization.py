"""Utilities for serializing MongoDB BSON types to JSON-compatible types."""

from datetime import datetime
from typing import Any, Dict, List, Union

from bson import ObjectId
from bson.decimal128 import Decimal128


def serialize_value(value: Any) -> Any:
    """Convert a single BSON value to a JSON-serializable type."""
    if isinstance(value, ObjectId):
        return str(value)
    elif isinstance(value, Decimal128):
        return float(value.to_decimal())
    elif isinstance(value, datetime):
        return value.isoformat()
    elif isinstance(value, dict):
        return serialize_document(value)
    elif isinstance(value, list):
        return [serialize_value(item) for item in value]
    return value


def serialize_document(doc: Dict[str, Any]) -> Dict[str, Any]:
    """
    Recursively serialize a MongoDB document to JSON-compatible types.

    Converts:
    - ObjectId -> str
    - Decimal128 -> float
    - datetime -> ISO string
    - Nested dicts and lists are processed recursively
    """
    if doc is None:
        return None

    result = {}
    for key, value in doc.items():
        result[key] = serialize_value(value)
    return result


def serialize_documents(docs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Serialize a list of MongoDB documents."""
    return [serialize_document(doc) for doc in docs]
