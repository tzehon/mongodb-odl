"""Pydantic models for request/response schemas."""

from datetime import datetime
from decimal import Decimal
from typing import Any, Generic, List, Optional, TypeVar
from pydantic import BaseModel, Field


# ============================================================================
# Address and Account Holder Models
# ============================================================================

class Address(BaseModel):
    """Physical address model."""
    line1: str
    line2: Optional[str] = None
    city: str
    postalCode: str = Field(alias="postal_code", default="")
    country: str

    class Config:
        populate_by_name = True


class AccountHolder(BaseModel):
    """Account holder information."""
    name: str
    email: str
    phone: str
    address: Address


# ============================================================================
# Branch and Statement Period Models
# ============================================================================

class Branch(BaseModel):
    """Bank branch information."""
    code: str
    name: str
    region: str


class StatementPeriod(BaseModel):
    """Statement period with start and end dates."""
    startDate: datetime = Field(alias="start_date")
    endDate: datetime = Field(alias="end_date")

    class Config:
        populate_by_name = True


# ============================================================================
# Transaction Models
# ============================================================================

class TransactionMetadata(BaseModel):
    """Transaction metadata including channel and location."""
    channel: str
    location: Optional[str] = None


class Transaction(BaseModel):
    """Individual transaction model."""
    transactionId: str = Field(alias="transaction_id")
    date: datetime
    description: str
    amount: float
    type: str  # "credit" or "debit"
    category: str
    merchant: str
    referenceNumber: str = Field(alias="reference_number")
    runningBalance: float = Field(alias="running_balance")
    metadata: TransactionMetadata

    class Config:
        populate_by_name = True


# ============================================================================
# Statement Models
# ============================================================================

class StatementMetadata(BaseModel):
    """Statement generation metadata."""
    generatedAt: datetime = Field(alias="generated_at")
    version: int
    source: str

    class Config:
        populate_by_name = True


class AccountStatement(BaseModel):
    """Complete account statement model."""
    accountNumber: str = Field(alias="account_number")
    accountHolder: AccountHolder = Field(alias="account_holder")
    accountType: str = Field(alias="account_type")
    branch: Branch
    currency: str
    statementPeriod: StatementPeriod = Field(alias="statement_period")
    openingBalance: float = Field(alias="opening_balance")
    closingBalance: float = Field(alias="closing_balance")
    transactions: List[Transaction]
    statementMetadata: StatementMetadata = Field(alias="statement_metadata")

    class Config:
        populate_by_name = True


class AccountStatementSummary(BaseModel):
    """Summarized account statement (without full transaction list)."""
    accountNumber: str
    accountHolder: AccountHolder
    accountType: str
    branch: Branch
    currency: str
    statementPeriod: StatementPeriod
    openingBalance: float
    closingBalance: float
    transactionCount: int
    totalCredits: float
    totalDebits: float
    statementMetadata: StatementMetadata


# ============================================================================
# Search Models
# ============================================================================

class TransactionSearchResult(BaseModel):
    """Search result for transaction search."""
    transactionId: str
    accountNumber: str
    date: datetime
    description: str
    amount: float
    type: str
    category: str
    merchant: str
    score: float = 0.0
    highlights: Optional[List[dict]] = None


# ============================================================================
# Balance Models
# ============================================================================

class BalanceResponse(BaseModel):
    """Current balance response."""
    accountNumber: str
    currentBalance: float
    currency: str
    asOf: datetime
    accountType: str


# ============================================================================
# Metrics Models
# ============================================================================

class MetricsResponse(BaseModel):
    """API metrics response."""
    currentQps: float
    averageLatencyMs: float
    p50LatencyMs: float
    p95LatencyMs: float
    p99LatencyMs: float
    documentCount: int
    connectionPoolStats: dict
    uptime: float
    timestamp: datetime


# ============================================================================
# Change Stream Models
# ============================================================================

class ChangeStreamEvent(BaseModel):
    """Change stream event model."""
    operationType: str  # "insert", "update", "replace", "delete"
    documentKey: dict
    timestamp: datetime
    fullDocument: Optional[dict] = None
    updateDescription: Optional[dict] = None
    clusterTime: Optional[datetime] = None


# ============================================================================
# Health Check Models
# ============================================================================

class HealthResponse(BaseModel):
    """Health check response."""
    status: str  # "healthy", "degraded", "unhealthy"
    mongodb: str
    timestamp: datetime
    version: str = "1.0.0"
    environment: str


# ============================================================================
# Pagination Models
# ============================================================================

T = TypeVar("T")


class PaginatedResponse(BaseModel, Generic[T]):
    """Generic paginated response wrapper."""
    items: List[T]
    total: int
    page: int
    pageSize: int
    totalPages: int
    hasNext: bool
    hasPrevious: bool


# ============================================================================
# Load Test Models
# ============================================================================

class LoadTestConfig(BaseModel):
    """Load test configuration."""
    concurrentUsers: int = Field(ge=1, le=100, default=10)
    durationSeconds: int = Field(ge=1, le=300, default=60)
    targetQps: Optional[int] = Field(ge=1, le=2000, default=None)


class LoadTestResult(BaseModel):
    """Load test result summary."""
    totalRequests: int
    successfulRequests: int
    failedRequests: int
    averageLatencyMs: float
    p50LatencyMs: float
    p95LatencyMs: float
    p99LatencyMs: float
    actualQps: float
    durationSeconds: float
    slaPassed: bool
    timestamp: datetime
