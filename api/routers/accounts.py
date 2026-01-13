"""Account-related API endpoints."""

from datetime import datetime
from typing import List, Optional
from bson.decimal128 import Decimal128
from fastapi import APIRouter, Depends, HTTPException, Query

from models import AccountStatement, AccountStatementSummary, BalanceResponse
from services import MongoDBService, get_mongodb_service


def to_float(value) -> float:
    """Convert Decimal128 or other numeric types to float."""
    if isinstance(value, Decimal128):
        return float(value.to_decimal())
    return float(value) if value is not None else 0.0

router = APIRouter(prefix="/api/v1/accounts", tags=["accounts"])


@router.get("", response_model=List[str])
async def list_accounts(
    limit: int = Query(default=100, ge=1, le=1000),
    mongodb: MongoDBService = Depends(get_mongodb_service)
):
    """
    List all account numbers.

    Returns a list of unique account numbers in the system.
    """
    return await mongodb.get_all_account_numbers(limit=limit)


@router.get("/{account_number}/statements/latest")
async def get_latest_statement(
    account_number: str,
    mongodb: MongoDBService = Depends(get_mongodb_service)
):
    """
    Get the latest statement for an account.

    Returns the most recent account statement including all transactions.
    """
    statement = await mongodb.get_latest_statement(account_number)

    if not statement:
        raise HTTPException(
            status_code=404,
            detail=f"No statement found for account {account_number}"
        )

    # Remove MongoDB _id field
    statement.pop("_id", None)
    return statement


@router.get("/{account_number}/statements")
async def get_statements(
    account_number: str,
    start_date: Optional[datetime] = Query(default=None),
    end_date: Optional[datetime] = Query(default=None),
    mongodb: MongoDBService = Depends(get_mongodb_service)
):
    """
    Get statements for an account within a date range.

    If no dates specified, returns all available statements.
    """
    # Default to last year if no dates specified
    if not start_date:
        start_date = datetime(2020, 1, 1)
    if not end_date:
        end_date = datetime.now()

    statements = await mongodb.get_statements_by_date_range(
        account_number, start_date, end_date
    )

    if not statements:
        raise HTTPException(
            status_code=404,
            detail=f"No statements found for account {account_number} in the specified date range"
        )

    # Remove MongoDB _id fields
    for stmt in statements:
        stmt.pop("_id", None)

    return statements


@router.get("/{account_number}/balance", response_model=BalanceResponse)
async def get_balance(
    account_number: str,
    mongodb: MongoDBService = Depends(get_mongodb_service)
):
    """
    Get current balance for an account.

    Returns the closing balance from the most recent statement.
    """
    balance = await mongodb.get_account_balance(account_number)

    if not balance:
        raise HTTPException(
            status_code=404,
            detail=f"No balance found for account {account_number}"
        )

    return balance


@router.get("/{account_number}/summary")
async def get_account_summary(
    account_number: str,
    mongodb: MongoDBService = Depends(get_mongodb_service)
):
    """
    Get account summary without full transaction list.

    Returns account details with transaction statistics.
    """
    statement = await mongodb.get_latest_statement(account_number)

    if not statement:
        raise HTTPException(
            status_code=404,
            detail=f"No data found for account {account_number}"
        )

    transactions = statement.get("transactions", [])

    # Calculate totals (convert Decimal128 to float)
    total_credits = sum(
        to_float(t.get("amount", 0)) for t in transactions if t.get("type") == "credit"
    )
    total_debits = sum(
        to_float(t.get("amount", 0)) for t in transactions if t.get("type") == "debit"
    )

    summary = {
        "accountNumber": statement.get("accountNumber"),
        "accountHolder": statement.get("accountHolder"),
        "accountType": statement.get("accountType"),
        "branch": statement.get("branch"),
        "currency": statement.get("currency"),
        "statementPeriod": statement.get("statementPeriod"),
        "openingBalance": to_float(statement.get("openingBalance")),
        "closingBalance": to_float(statement.get("closingBalance")),
        "transactionCount": len(transactions),
        "totalCredits": total_credits,
        "totalDebits": total_debits,
        "statementMetadata": statement.get("statementMetadata")
    }

    return summary
