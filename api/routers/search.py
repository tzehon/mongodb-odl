"""Search-related API endpoints using Atlas Search."""

from typing import List, Optional
from fastapi import APIRouter, Depends, HTTPException, Query

from models import TransactionSearchResult
from services import MongoDBService, get_mongodb_service
from utils import serialize_documents

router = APIRouter(prefix="/api/v1/accounts", tags=["search"])


@router.get("/{account_number}/transactions/search")
async def search_transactions(
    account_number: str,
    q: str = Query(..., min_length=1, description="Search query"),
    limit: int = Query(default=20, ge=1, le=100),
    fuzzy: bool = Query(default=True, description="Enable fuzzy matching"),
    mongodb: MongoDBService = Depends(get_mongodb_service)
):
    """
    Search transactions using full-text search.

    Searches across:
    - Transaction descriptions
    - Merchant names

    Features:
    - Fuzzy matching for typo tolerance
    - Relevance scoring
    - Highlighted results

    Example queries:
    - /search?q=fairprice → Returns all FairPrice transactions
    - /search?q=grab → Returns Grab transactions (transport + food)
    - /search?q=utilties → Fuzzy matches "utilities"
    """
    # Verify account exists (skip full check in benchmark mode for speed)
    statement = await mongodb.get_latest_statement(account_number)
    if not statement:
        raise HTTPException(
            status_code=404,
            detail=f"Account {account_number} not found"
        )

    results = await mongodb.search_transactions(
        account_number=account_number,
        query=q,
        limit=limit,
        fuzzy=fuzzy
    )

    # In benchmark mode, pass through the benchmark response with _dbExecTimeMs
    if isinstance(results, dict) and results.get("_benchmark"):
        return results

    return {
        "query": q,
        "accountNumber": account_number,
        "resultCount": len(results),
        "results": serialize_documents(results)
    }


@router.get("/{account_number}/transactions/autocomplete")
async def autocomplete_search(
    account_number: str,
    q: str = Query(..., min_length=2, description="Partial search query"),
    limit: int = Query(default=10, ge=1, le=50),
    mongodb: MongoDBService = Depends(get_mongodb_service)
):
    """
    Autocomplete search for transaction descriptions and merchants.

    Use this for search-as-you-type functionality.
    Returns suggestions based on partial input.
    """
    collection = mongodb.get_collection("account_statements")

    # Try Atlas Search autocomplete first
    try:
        pipeline = [
            {
                "$search": {
                    "index": "transaction_autocomplete",
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
                                "autocomplete": {
                                    "query": q,
                                    "path": "transactions.merchant",
                                    "tokenOrder": "any"
                                }
                            },
                            {
                                "autocomplete": {
                                    "query": q,
                                    "path": "transactions.description",
                                    "tokenOrder": "any"
                                }
                            }
                        ],
                        "minimumShouldMatch": 1
                    }
                }
            },
            {"$unwind": "$transactions"},
            {
                "$match": {
                    "$or": [
                        {"transactions.merchant": {"$regex": f"^{q}", "$options": "i"}},
                        {"transactions.description": {"$regex": q, "$options": "i"}}
                    ]
                }
            },
            {
                "$group": {
                    "_id": {
                        "merchant": "$transactions.merchant",
                        "category": "$transactions.category"
                    }
                }
            },
            {"$limit": limit},
            {
                "$project": {
                    "merchant": "$_id.merchant",
                    "category": "$_id.category",
                    "_id": 0
                }
            }
        ]

        results = await collection.aggregate(pipeline).to_list(length=limit)

    except Exception:
        # Fallback to regex-based autocomplete
        pipeline = [
            {"$match": {"accountNumber": account_number}},
            {"$unwind": "$transactions"},
            {
                "$match": {
                    "$or": [
                        {"transactions.merchant": {"$regex": q, "$options": "i"}},
                        {"transactions.description": {"$regex": q, "$options": "i"}}
                    ]
                }
            },
            {
                "$group": {
                    "_id": {
                        "merchant": "$transactions.merchant",
                        "category": "$transactions.category"
                    }
                }
            },
            {"$limit": limit},
            {
                "$project": {
                    "merchant": "$_id.merchant",
                    "category": "$_id.category",
                    "_id": 0
                }
            }
        ]

        results = await collection.aggregate(pipeline).to_list(length=limit)

    return {
        "query": q,
        "suggestions": results
    }


@router.get("/search/global")
async def global_search(
    q: str = Query(..., min_length=1, description="Search query"),
    limit: int = Query(default=20, ge=1, le=100),
    mongodb: MongoDBService = Depends(get_mongodb_service)
):
    """
    Search across all accounts (admin/analytics use).

    Returns transactions from any account matching the query.
    Useful for fraud detection and pattern analysis.
    """
    collection = mongodb.get_collection("account_statements")

    try:
        # Atlas Search across all documents
        pipeline = [
            {
                "$search": {
                    "index": "transaction_search",
                    "compound": {
                        "should": [
                            {
                                "text": {
                                    "query": q,
                                    "path": "transactions.description",
                                    "fuzzy": {"maxEdits": 2},
                                    "score": {"boost": {"value": 2}}
                                }
                            },
                            {
                                "text": {
                                    "query": q,
                                    "path": "transactions.merchant",
                                    "fuzzy": {"maxEdits": 1}
                                }
                            }
                        ],
                        "minimumShouldMatch": 1
                    }
                }
            },
            {"$unwind": "$transactions"},
            {
                "$match": {
                    "$or": [
                        {"transactions.description": {"$regex": q, "$options": "i"}},
                        {"transactions.merchant": {"$regex": q, "$options": "i"}}
                    ]
                }
            },
            {
                "$project": {
                    "accountNumber": 1,
                    "transactionId": "$transactions.transactionId",
                    "date": "$transactions.date",
                    "description": "$transactions.description",
                    "amount": "$transactions.amount",
                    "type": "$transactions.type",
                    "category": "$transactions.category",
                    "merchant": "$transactions.merchant",
                    "score": {"$meta": "searchScore"}
                }
            },
            {"$sort": {"score": -1}},
            {"$limit": limit}
        ]

        results = await collection.aggregate(pipeline).to_list(length=limit)

    except Exception:
        # Fallback to regex search
        pipeline = [
            {"$unwind": "$transactions"},
            {
                "$match": {
                    "$or": [
                        {"transactions.description": {"$regex": q, "$options": "i"}},
                        {"transactions.merchant": {"$regex": q, "$options": "i"}}
                    ]
                }
            },
            {
                "$project": {
                    "accountNumber": 1,
                    "transactionId": "$transactions.transactionId",
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

        results = await collection.aggregate(pipeline).to_list(length=limit)

    return {
        "query": q,
        "resultCount": len(results),
        "results": serialize_documents(results)
    }
