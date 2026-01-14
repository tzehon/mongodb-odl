"""Main FastAPI application for the ODL Banking API."""

import logging
import time
from contextlib import asynccontextmanager
from datetime import datetime

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from prometheus_fastapi_instrumentator import Instrumentator

from config import settings
from routers import accounts_router, transactions_router, search_router, metrics_router
from routers.metrics import metrics_collector
from websocket import websocket_router
from services.mongodb import get_mongodb_service, close_mongodb_service
from services import set_metrics_collector, get_request_db_exec_time, set_current_request, clear_current_request, is_benchmark_mode

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager."""
    # Startup
    logger.info("Starting ODL Banking API...")
    logger.info(f"Environment: {settings.environment}")

    # Initialize metrics collector for DB latency tracking
    set_metrics_collector(metrics_collector)
    logger.info("Metrics collector initialized for DB latency tracking")

    # Connect to MongoDB
    try:
        mongodb = await get_mongodb_service()
        logger.info("MongoDB connection established")
    except Exception as e:
        logger.error(f"Failed to connect to MongoDB: {e}")
        raise

    yield

    # Shutdown
    logger.info("Shutting down ODL Banking API...")
    await close_mongodb_service()
    logger.info("MongoDB connection closed")


# Create FastAPI application
app = FastAPI(
    title="MongoDB Atlas ODL Banking API",
    description="""
    Banking Operational Data Layer API demonstrating real-time data sync
    from Databricks Lakehouse to MongoDB Atlas.

    ## Features
    - Real-time account statement queries
    - Transaction filtering and search
    - Full-text search with Atlas Search
    - Real-time CDC via Change Streams (WebSocket)
    - Performance metrics and monitoring

    ## Target SLA
    - 500 QPS (queries per second)
    - < 100ms p95 latency
    """,
    version="1.0.0",
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc"
)

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins.split(","),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Request timing middleware
@app.middleware("http")
async def timing_middleware(request: Request, call_next):
    """Middleware to track request timing for metrics."""
    start_time = time.time()

    # Set current request reference so DB operations can store exec time
    set_current_request(request)
    request.state.db_exec_time = 0.0

    try:
        response = await call_next(request)
        success = response.status_code < 400
    except Exception as e:
        success = False
        raise
    finally:
        # Calculate latency
        latency_ms = (time.time() - start_time) * 1000

        # Record metrics (skip health check and metrics endpoints)
        if not request.url.path.startswith(("/health", "/metrics", "/docs", "/redoc", "/openapi")):
            metrics_collector.record_request(latency_ms, success)

        # Clear current request reference
        clear_current_request()

    # Add DB execution time header (for Locust to use in benchmark mode)
    if is_benchmark_mode():
        db_exec_time = getattr(request.state, 'db_exec_time', 0.0)
        response.headers["X-DB-Execution-Time-Ms"] = str(round(db_exec_time, 2))

    return response


# Include routers
app.include_router(accounts_router)
app.include_router(transactions_router)
app.include_router(search_router)
app.include_router(metrics_router)
app.include_router(websocket_router)

# Setup Prometheus metrics
if settings.enable_metrics:
    Instrumentator().instrument(app).expose(app, endpoint=settings.metrics_path)


# Health check endpoint
@app.get("/health", tags=["health"])
async def health_check():
    """
    Health check endpoint.

    Returns the health status of the API and its dependencies.
    """
    try:
        mongodb = await get_mongodb_service()
        mongodb_healthy = await mongodb.is_healthy()
    except Exception:
        mongodb_healthy = False

    status = "healthy" if mongodb_healthy else "unhealthy"

    return {
        "status": status,
        "mongodb": "connected" if mongodb_healthy else "disconnected",
        "timestamp": datetime.utcnow().isoformat(),
        "version": "1.0.0",
        "environment": settings.environment
    }


@app.get("/", tags=["root"])
async def root():
    """Root endpoint with API information."""
    return {
        "name": "MongoDB Atlas ODL Banking API",
        "version": "1.0.0",
        "description": "Banking Operational Data Layer API",
        "docs": "/docs",
        "health": "/health",
        "metrics": "/api/v1/metrics"
    }


# Error handlers
@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    """Global exception handler."""
    logger.error(f"Unhandled exception: {exc}", exc_info=True)
    return JSONResponse(
        status_code=500,
        content={
            "error": "Internal server error",
            "detail": str(exc) if settings.api_debug else "An unexpected error occurred",
            "timestamp": datetime.utcnow().isoformat()
        }
    )


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "main:app",
        host=settings.api_host,
        port=settings.api_port,
        reload=settings.api_debug,
        log_level="info"
    )
