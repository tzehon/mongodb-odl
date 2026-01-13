"""API Routers for the ODL Banking API."""

from .accounts import router as accounts_router
from .transactions import router as transactions_router
from .search import router as search_router
from .metrics import router as metrics_router

__all__ = [
    "accounts_router",
    "transactions_router",
    "search_router",
    "metrics_router",
]
