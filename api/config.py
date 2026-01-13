"""Configuration management for the ODL Banking API."""

from functools import lru_cache
from pydantic_settings import BaseSettings
from typing import Optional


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    # MongoDB Configuration
    mongodb_uri: str = "mongodb://localhost:27017"
    mongodb_database: str = "banking_odl"

    # MongoDB Connection Pool Settings
    mongodb_min_pool_size: int = 10
    mongodb_max_pool_size: int = 100
    mongodb_max_idle_time_ms: int = 30000
    mongodb_wait_queue_timeout_ms: int = 5000
    mongodb_server_selection_timeout_ms: int = 5000

    # API Configuration
    api_host: str = "0.0.0.0"
    api_port: int = 8000
    api_debug: bool = False

    # Environment
    environment: str = "development"

    # CORS Configuration
    cors_origins: str = "*"

    # Metrics
    enable_metrics: bool = True
    metrics_path: str = "/metrics"

    # Change Streams
    change_stream_batch_size: int = 100
    change_stream_max_await_time_ms: int = 1000

    # Rate Limiting (for load testing awareness)
    rate_limit_enabled: bool = False
    rate_limit_requests_per_second: int = 1000

    # Databricks (optional - for status checks)
    databricks_host: Optional[str] = None
    databricks_token: Optional[str] = None

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = False


@lru_cache()
def get_settings() -> Settings:
    """Get cached settings instance."""
    return Settings()


# Convenience exports
settings = get_settings()
