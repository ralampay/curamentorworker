"""Environment-backed configuration helpers."""

from dataclasses import dataclass
import os
from typing import Optional


def _as_int(value: Optional[str], default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


@dataclass
class Settings:
    """Runtime settings for the worker."""

    app_env: str = os.getenv("APP_ENV", "development")
    aws_region: str = os.getenv("AWS_REGION", "us-east-1")
    sqs_queue_url: str = os.getenv("AWS_SQS_QUEUE_URL", "")
    s3_bucket_name: str = os.getenv("S3_BUCKET_NAME", "")
    s3_prefix: str = os.getenv("S3_PREFIX", "")
    db_host: str = os.getenv("DB_HOST", "localhost")
    db_port: int = _as_int(os.getenv("DB_PORT"), 5432)
    db_name: str = os.getenv("DB_NAME", "curamentor")
    db_user: str = os.getenv("DB_USER", "")
    db_password: str = os.getenv("DB_PASSWORD", "")
    llama_model_path: str = os.getenv("LLAMA_MODEL_PATH", "/models/llama-13b.ggmlv3.q4_0.bin")
    poll_interval_seconds: int = _as_int(os.getenv("POLL_INTERVAL_SECONDS"), 5)
    max_messages: int = _as_int(os.getenv("MAX_MESSAGES"), 1)
    visibility_timeout: int = _as_int(os.getenv("SQS_VISIBILITY_TIMEOUT"), 30)

    def validate(self) -> None:
        """Ensure all required settings exist before running."""
        missing = [name for name in ("sqs_queue_url", "db_user", "db_password") if not getattr(self, name)]
        if missing:
            raise RuntimeError(f"Missing required configuration: {', '.join(missing)}")
