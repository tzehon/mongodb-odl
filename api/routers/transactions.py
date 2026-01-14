"""Transaction-related API endpoints."""

from datetime import datetime
from typing import Optional
from bson.decimal128 import Decimal128
from fastapi import APIRouter, Depends, HTTPException, Query

from services import MongoDBService, get_mongodb_service
from utils import serialize_documents


def to_float(value) -> float:
    """Convert Decimal128 or other numeric types to float."""
    if isinstance(value, Decimal128):
        return float(value.to_decimal())
    return float(value) if value is not None else 0.0

router = APIRouter(prefix="/api/v1/accounts", tags=["transactions"])


@router.get("/{account_number}/transactions")
async def get_transactions(
    account_number: str,
    limit: int = Query(default=50, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
    start_date: Optional[datetime] = Query(default=None, description="Filter by start date"),
    end_date: Optional[datetime] = Query(default=None, description="Filter by end date"),
    min_amount: Optional[float] = Query(default=None, description="Minimum transaction amount"),
    max_amount: Optional[float] = Query(default=None, description="Maximum transaction amount"),
    category: Optional[str] = Query(default=None, description="Filter by category"),
    type: Optional[str] = Query(default=None, description="Filter by type (credit/debit)"),
    mongodb: MongoDBService = Depends(get_mongodb_service)
):
    """
    Get transactions for an account with filtering and pagination.

    Supports filtering by:
    - Date range (start_date, end_date)
    - Amount range (min_amount, max_amount)
    - Category
    - Type (credit/debit)

    Returns paginated results with metadata.
    """
    result = await mongodb.get_transactions(
        account_number=account_number,
        limit=limit,
        offset=offset,
        start_date=start_date,
        end_date=end_date,
        min_amount=min_amount,
        max_amount=max_amount,
        category=category,
        tx_type=type
    )

    # In benchmark mode, return the result directly (contains _dbExecTimeMs)
    if result.get("_benchmark"):
        return result

    if not result["items"] and result["total"] == 0:
        # Check if account exists at all
        statement = await mongodb.get_latest_statement(account_number)
        if not statement:
            raise HTTPException(
                status_code=404,
                detail=f"Account {account_number} not found"
            )

    # Serialize BSON types (Decimal128, ObjectId) to JSON-compatible types
    result["items"] = serialize_documents(result["items"])
    return result


@router.get("/{account_number}/transactions/categories")
async def get_transaction_categories(
    account_number: str,
    mongodb: MongoDBService = Depends(get_mongodb_service)
):
    """
    Get unique transaction categories for an account.

    Useful for populating filter dropdowns.
    """
    collection = mongodb.get_collection("account_statements")

    pipeline = [
        {"$match": {"accountNumber": account_number}},
        {"$unwind": "$transactions"},
        {"$group": {"_id": "$transactions.category"}},
        {"$sort": {"_id": 1}}
    ]

    results = await collection.aggregate(pipeline).to_list(length=100)
    categories = [r["_id"] for r in results if r["_id"]]

    if not categories:
        statement = await mongodb.get_latest_statement(account_number)
        if not statement:
            raise HTTPException(
                status_code=404,
                detail=f"Account {account_number} not found"
            )

    return {"categories": categories}


@router.get("/{account_number}/transactions/merchants")
async def get_transaction_merchants(
    account_number: str,
    limit: int = Query(default=50, ge=1, le=200),
    mongodb: MongoDBService = Depends(get_mongodb_service)
):
    """
    Get unique merchants for an account with transaction counts.

    Useful for analytics and filtering.
    """
    collection = mongodb.get_collection("account_statements")

    pipeline = [
        {"$match": {"accountNumber": account_number}},
        {"$unwind": "$transactions"},
        {
            "$group": {
                "_id": "$transactions.merchant",
                "count": {"$sum": 1},
                "totalAmount": {"$sum": "$transactions.amount"}
            }
        },
        {"$sort": {"count": -1}},
        {"$limit": limit},
        {
            "$project": {
                "merchant": "$_id",
                "transactionCount": "$count",
                "totalAmount": {"$round": ["$totalAmount", 2]},
                "_id": 0
            }
        }
    ]

    results = await collection.aggregate(pipeline).to_list(length=limit)
    return {"merchants": serialize_documents(results)}


@router.get("/{account_number}/transactions/stats")
async def get_transaction_stats(
    account_number: str,
    start_date: Optional[datetime] = Query(default=None),
    end_date: Optional[datetime] = Query(default=None),
    mongodb: MongoDBService = Depends(get_mongodb_service)
):
    """
    Get transaction statistics for an account.

    Returns aggregated statistics including:
    - Total credits and debits
    - Average transaction amount
    - Transaction count by category
    - Transaction count by type
    """
    collection = mongodb.get_collection("account_statements")

    # Build match stage
    match_stage = {"accountNumber": account_number}

    pipeline = [
        {"$match": match_stage},
        {"$unwind": "$transactions"},
    ]

    # Add date filter if specified
    if start_date or end_date:
        date_filter = {}
        if start_date:
            date_filter["$gte"] = start_date
        if end_date:
            date_filter["$lte"] = end_date
        pipeline.append({"$match": {"transactions.date": date_filter}})

    # Add statistics aggregation
    pipeline.append({
        "$group": {
            "_id": None,
            "totalTransactions": {"$sum": 1},
            "totalCredits": {
                "$sum": {
                    "$cond": [
                        {"$eq": ["$transactions.type", "credit"]},
                        "$transactions.amount",
                        0
                    ]
                }
            },
            "totalDebits": {
                "$sum": {
                    "$cond": [
                        {"$eq": ["$transactions.type", "debit"]},
                        "$transactions.amount",
                        0
                    ]
                }
            },
            "creditCount": {
                "$sum": {"$cond": [{"$eq": ["$transactions.type", "credit"]}, 1, 0]}
            },
            "debitCount": {
                "$sum": {"$cond": [{"$eq": ["$transactions.type", "debit"]}, 1, 0]}
            },
            "avgAmount": {"$avg": "$transactions.amount"},
            "maxAmount": {"$max": "$transactions.amount"},
            "minAmount": {"$min": "$transactions.amount"}
        }
    })

    results = await collection.aggregate(pipeline).to_list(length=1)

    if not results:
        statement = await mongodb.get_latest_statement(account_number)
        if not statement:
            raise HTTPException(
                status_code=404,
                detail=f"Account {account_number} not found"
            )
        return {
            "totalTransactions": 0,
            "totalCredits": 0,
            "totalDebits": 0,
            "creditCount": 0,
            "debitCount": 0,
            "avgAmount": 0,
            "maxAmount": 0,
            "minAmount": 0
        }

    stats = results[0]
    stats.pop("_id", None)

    # Convert Decimal128 values to float and round
    for key in ["totalCredits", "totalDebits", "avgAmount", "maxAmount", "minAmount"]:
        if stats.get(key) is not None:
            stats[key] = round(to_float(stats[key]), 2)

    return stats


@router.get("/{account_number}/transactions/by-category")
async def get_transactions_by_category(
    account_number: str,
    mongodb: MongoDBService = Depends(get_mongodb_service)
):
    """
    Get transaction breakdown by category.

    Returns count and total amount for each category.
    """
    collection = mongodb.get_collection("account_statements")

    pipeline = [
        {"$match": {"accountNumber": account_number}},
        {"$unwind": "$transactions"},
        {
            "$group": {
                "_id": "$transactions.category",
                "count": {"$sum": 1},
                "totalAmount": {"$sum": "$transactions.amount"},
                "avgAmount": {"$avg": "$transactions.amount"}
            }
        },
        {"$sort": {"totalAmount": -1}},
        {
            "$project": {
                "category": "$_id",
                "transactionCount": "$count",
                "totalAmount": {"$round": ["$totalAmount", 2]},
                "avgAmount": {"$round": ["$avgAmount", 2]},
                "_id": 0
            }
        }
    ]

    results = await collection.aggregate(pipeline).to_list(length=100)
    return {"breakdown": serialize_documents(results)}
