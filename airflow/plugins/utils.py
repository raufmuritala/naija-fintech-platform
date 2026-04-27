"""
airflow/plugins/utils.py
─────────────────────────
Shared helpers used across all DAGs.
Populated incrementally as each phase adds utilities.
"""

import os
import logging
from datetime import datetime, timezone

logger = logging.getLogger(__name__)


def get_env(key: str, required: bool = True) -> str:
    """Fetch an env var, raising clearly if required and missing."""
    val = os.getenv(key)
    if required and not val:
        raise EnvironmentError(
            f"Required environment variable '{key}' is not set. "
            f"Check your .env file and docker-compose.yml."
        )
    return val or ""


def gcs_key(table: str, ds: str, suffix: str = "parquet") -> str:
    """
    Build a consistent GCS object path.
    Example: raw/transactions/dt=2024-01-15/transactions.parquet
    """
    return f"raw/{table}/dt={ds}/{table}.{suffix}"


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def log_task_start(task_name: str, **kwargs) -> None:
    logger.info("=" * 60)
    logger.info(f"Starting task: {task_name}")
    for k, v in kwargs.items():
        logger.info(f"  {k}: {v}")
    logger.info("=" * 60)
