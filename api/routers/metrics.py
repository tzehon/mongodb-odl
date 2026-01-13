"""Metrics and monitoring endpoints."""

import time
from collections import deque
from datetime import datetime, timezone
from typing import Deque, Dict, List
from fastapi import APIRouter, Depends
import statistics

from config import settings
from services import MongoDBService, get_mongodb_service

router = APIRouter(prefix="/api/v1", tags=["metrics"])


# In-memory metrics storage (for demo purposes)
# In production, use Prometheus or similar
class MetricsCollector:
    """Collects and stores request metrics."""

    def __init__(self, max_samples: int = 10000):
        self.max_samples = max_samples
        self.latencies: Deque[float] = deque(maxlen=max_samples)
        self.request_times: Deque[float] = deque(maxlen=max_samples)
        self.start_time = time.time()
        self._total_requests = 0
        self._failed_requests = 0

    def record_request(self, latency_ms: float, success: bool = True):
        """Record a request with its latency."""
        self.latencies.append(latency_ms)
        self.request_times.append(time.time())
        self._total_requests += 1
        if not success:
            self._failed_requests += 1

    def get_qps(self, window_seconds: float = 5.0) -> float:
        """Calculate QPS over a time window."""
        now = time.time()
        cutoff = now - window_seconds
        recent_count = sum(1 for t in self.request_times if t > cutoff)
        return recent_count / window_seconds

    def get_latency_percentiles(self) -> Dict[str, float]:
        """Get latency percentiles."""
        if not self.latencies:
            return {"p50": 0, "p95": 0, "p99": 0, "avg": 0}

        sorted_latencies = sorted(self.latencies)
        n = len(sorted_latencies)

        return {
            "p50": sorted_latencies[int(n * 0.50)] if n > 0 else 0,
            "p95": sorted_latencies[int(n * 0.95)] if n > 0 else 0,
            "p99": sorted_latencies[int(n * 0.99)] if n > 0 else 0,
            "avg": statistics.mean(sorted_latencies) if sorted_latencies else 0
        }

    def get_uptime(self) -> float:
        """Get uptime in seconds."""
        return time.time() - self.start_time

    def get_error_rate(self) -> float:
        """Get error rate as a percentage."""
        if self._total_requests == 0:
            return 0
        return (self._failed_requests / self._total_requests) * 100

    def reset(self):
        """Reset all metrics."""
        self.latencies.clear()
        self.request_times.clear()
        self._total_requests = 0
        self._failed_requests = 0


# Global metrics collector
metrics_collector = MetricsCollector()


def get_metrics_collector() -> MetricsCollector:
    """Get the metrics collector instance."""
    return metrics_collector


# Sync latency tracker - stores the actual measured sync latency
class SyncLatencyTracker:
    """Tracks actual sync latency when new documents arrive."""

    def __init__(self):
        self.last_doc_count = 0
        self.last_generated_at = None
        self.measured_latency_seconds = None  # The actual sync latency we measured
        self.measurement_time = None  # When we measured it

    def update(self, doc_count: int, generated_at: datetime) -> float:
        """
        Update tracker with current state.
        Returns the measured sync latency if new documents arrived.
        """
        now = datetime.now(timezone.utc)

        # If new documents arrived, measure the latency
        if doc_count > self.last_doc_count and generated_at:
            # Ensure generated_at is timezone-aware for comparison
            if generated_at.tzinfo is None:
                # If naive, assume UTC
                generated_at = generated_at.replace(tzinfo=timezone.utc)
            self.measured_latency_seconds = (now - generated_at).total_seconds()
            self.measurement_time = now

        self.last_doc_count = doc_count
        self.last_generated_at = generated_at

        return self.measured_latency_seconds


sync_latency_tracker = SyncLatencyTracker()


@router.get("/metrics")
async def get_metrics(
    mongodb: MongoDBService = Depends(get_mongodb_service),
    collector: MetricsCollector = Depends(get_metrics_collector)
):
    """
    Get current API metrics.

    Returns:
    - Current QPS (queries per second)
    - Latency percentiles (p50, p95, p99)
    - Document count
    - Connection pool stats
    - Uptime
    - SLA compliance status
    """
    # Get latency percentiles
    percentiles = collector.get_latency_percentiles()

    # Get document count
    doc_count = await mongodb.get_document_count()

    # Get connection pool stats
    pool_stats = await mongodb.get_connection_pool_stats()

    # Calculate QPS
    current_qps = collector.get_qps()

    # Check SLA compliance
    sla_qps_target = 500
    sla_latency_target = 100  # ms

    metrics = {
        "currentQps": round(current_qps, 2),
        "averageLatencyMs": round(percentiles["avg"], 2),
        "p50LatencyMs": round(percentiles["p50"], 2),
        "p95LatencyMs": round(percentiles["p95"], 2),
        "p99LatencyMs": round(percentiles["p99"], 2),
        "documentCount": doc_count,
        "connectionPoolStats": pool_stats,
        "uptime": round(collector.get_uptime(), 2),
        "errorRate": round(collector.get_error_rate(), 2),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "sla": {
            "qpsTarget": sla_qps_target,
            "latencyTargetMs": sla_latency_target,
            "qpsMet": current_qps >= sla_qps_target * 0.9,  # Allow 10% tolerance
            "latencyMet": percentiles["p95"] <= sla_latency_target,
            "overallPassed": (
                current_qps >= sla_qps_target * 0.9 and
                percentiles["p95"] <= sla_latency_target
            )
        }
    }

    return metrics


@router.get("/metrics/history")
async def get_metrics_history(
    window_minutes: int = 5,
    collector: MetricsCollector = Depends(get_metrics_collector)
):
    """
    Get historical metrics for charting.

    Returns time-series data for QPS and latency over the specified window.
    """
    now = time.time()
    window_seconds = window_minutes * 60
    bucket_seconds = 10  # 10-second buckets

    # Group requests into time buckets
    buckets: Dict[int, List[float]] = {}

    for i, (request_time, latency) in enumerate(
        zip(collector.request_times, collector.latencies)
    ):
        if request_time > now - window_seconds:
            bucket = int((request_time - (now - window_seconds)) / bucket_seconds)
            if bucket not in buckets:
                buckets[bucket] = []
            buckets[bucket].append(latency)

    # Calculate metrics per bucket
    history = []
    num_buckets = int(window_seconds / bucket_seconds)

    for i in range(num_buckets):
        bucket_latencies = buckets.get(i, [])
        bucket_time = datetime.fromtimestamp(now - window_seconds + (i * bucket_seconds))

        if bucket_latencies:
            sorted_lats = sorted(bucket_latencies)
            n = len(sorted_lats)
            avg_latency = statistics.mean(sorted_lats)
            p95_latency = sorted_lats[int(n * 0.95)] if n > 0 else 0
        else:
            avg_latency = 0
            p95_latency = 0

        history.append({
            "timestamp": bucket_time.isoformat(),
            "qps": len(bucket_latencies) / bucket_seconds,
            "avgLatencyMs": round(avg_latency, 2),
            "p95LatencyMs": round(p95_latency, 2),
            "requestCount": len(bucket_latencies)
        })

    return {
        "windowMinutes": window_minutes,
        "bucketSeconds": bucket_seconds,
        "dataPoints": history
    }


@router.post("/metrics/reset")
async def reset_metrics(
    collector: MetricsCollector = Depends(get_metrics_collector)
):
    """Reset all metrics (useful before load testing)."""
    collector.reset()
    return {"status": "ok", "message": "Metrics reset successfully"}


@router.get("/sync/status")
async def get_sync_status(
    mongodb: MongoDBService = Depends(get_mongodb_service)
):
    """
    Get data synchronization status.

    Shows information about the latest synced data from Databricks.
    Returns:
    - pipelineLatencySeconds: True pipeline latency (time from Spark batch processing to detection)
    - timeSinceLastSync: Seconds since the last document was synced (for idle detection)
    """
    collection = mongodb.get_collection("account_statements")

    # Get the most recently processed document (sorted by _processedAt)
    latest = await collection.find_one(
        {},
        sort=[("_processedAt", -1)],
        projection={
            "accountNumber": 1,
            "statementMetadata": 1,
            "_batchStartTime": 1,
            "_processedAt": 1,
            "_updated_at": 1
        }
    )

    # Get document count
    doc_count = await mongodb.get_document_count()

    # Get timestamps
    last_generated_at = None
    batch_start_time = None
    processed_at = None
    pipeline_latency = None

    if latest:
        # generatedAt - when data was created in Databricks (for idle detection)
        if latest.get("statementMetadata", {}).get("generatedAt"):
            last_generated_at = latest["statementMetadata"]["generatedAt"]
            if not isinstance(last_generated_at, datetime):
                last_generated_at = None

        # _batchStartTime (T1) - when Spark batch started processing
        if latest.get("_batchStartTime"):
            batch_start_str = latest["_batchStartTime"]
            if isinstance(batch_start_str, str):
                try:
                    batch_start_time = datetime.fromisoformat(batch_start_str.replace('Z', '+00:00'))
                except ValueError:
                    batch_start_time = None
            elif isinstance(batch_start_str, datetime):
                batch_start_time = batch_start_str

        # _processedAt (T2) - when data was written to MongoDB
        if latest.get("_processedAt"):
            processed_str = latest["_processedAt"]
            if isinstance(processed_str, str):
                try:
                    processed_at = datetime.fromisoformat(processed_str.replace('Z', '+00:00'))
                except ValueError:
                    processed_at = None
            elif isinstance(processed_str, datetime):
                processed_at = processed_str

        # Calculate Pipeline Latency = T2 - T1 (batch processing + MongoDB write time)
        if batch_start_time and processed_at:
            pipeline_latency = (processed_at - batch_start_time).total_seconds()

    # Update tracker for change detection
    sync_latency_tracker.update(doc_count, processed_at)

    # Calculate time since last sync (for idle detection) - uses generatedAt
    time_since_last_sync = None
    if last_generated_at:
        # Ensure last_generated_at is timezone-aware for comparison
        if last_generated_at.tzinfo is None:
            last_generated_at = last_generated_at.replace(tzinfo=timezone.utc)
        time_since_last_sync = (datetime.now(timezone.utc) - last_generated_at).total_seconds()

    return {
        "status": "connected",
        "documentCount": doc_count,
        "lastSyncedDocument": {
            "accountNumber": latest.get("accountNumber") if latest else None,
            "generatedAt": last_generated_at.isoformat() if last_generated_at else None,
            "batchStartTime": batch_start_time.isoformat() if batch_start_time else None,
            "processedAt": processed_at.isoformat() if processed_at else None,
            "source": latest.get("statementMetadata", {}).get("source") if latest else None
        },
        # Pipeline Latency = T2 - T1 (Spark batch processing + MongoDB write)
        # This is the actual time Spark took to process and write the data
        "pipelineLatencySeconds": round(pipeline_latency, 2) if pipeline_latency is not None else None,
        # Keep syncLatencySeconds for backward compatibility
        "syncLatencySeconds": round(pipeline_latency, 2) if pipeline_latency is not None else None,
        # Time since last sync (for idle detection)
        "timeSinceLastSync": round(time_since_last_sync, 2) if time_since_last_sync else None,
        "timestamp": datetime.now(timezone.utc).isoformat()
    }
