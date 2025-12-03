"""Environment-backed configuration helpers."""

from dataclasses import dataclass, field
import os
from typing import Optional

_LOCALSTACK_ACCOUNT_ID = "000000000000"


def _as_int(value: Optional[str], default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _queue_name_from_url(queue_url: str) -> Optional[str]:
    """Return the trailing queue name segment from a fully-qualified URL."""
    if not queue_url:
        return None
    trimmed = queue_url.rstrip("/")
    if not trimmed:
        return None
    segments = trimmed.split("/")
    return segments[-1] if segments else None


def _env(key: str, default: Optional[str] = None) -> Optional[str]:
    """Helper so dataclass default_factory can resolve vars lazily."""
    return os.getenv(key, default)


def _env_int(key: str, default: int) -> int:
    return _as_int(os.getenv(key), default)


@dataclass
class Settings:
    """Runtime settings for the worker."""

    app_env: str = field(default_factory=lambda: _env("APP_ENV", "development") or "development")
    aws_region: str = field(default_factory=lambda: _env("AWS_REGION", "us-east-1") or "us-east-1")
    aws_sqs_queue_url: str = field(default_factory=lambda: _env("AWS_SQS_QUEUE_URL", "") or "")
    aws_sqs_queue_name: str = field(default_factory=lambda: _env("AWS_SQS_QUEUE_NAME", "") or "")
    localstack_url: str = field(default_factory=lambda: _env("LOCALSTACK_URL", "") or "")
    s3_bucket_name: str = field(default_factory=lambda: _env("S3_BUCKET_NAME", "") or "")
    s3_prefix: str = field(default_factory=lambda: _env("S3_PREFIX", "") or "")
    db_host: str = field(default_factory=lambda: _env("DB_HOST", "localhost") or "localhost")
    db_port: int = field(default_factory=lambda: _env_int("DB_PORT", 5432))
    db_name: str = field(default_factory=lambda: _env("DB_NAME", "curamentor") or "curamentor")
    db_user: str = field(default_factory=lambda: _env("DB_USER", "") or "")
    db_password: str = field(default_factory=lambda: _env("DB_PASSWORD", "") or "")
    llama_model_path: str = field(
        default_factory=lambda: _env("LLAMA_MODEL_PATH", "/models/llama-13b.ggmlv3.q4_0.bin")
        or "/models/llama-13b.ggmlv3.q4_0.bin"
    )
    openai_api_key: str = field(default_factory=lambda: _env("OPENAI_API_KEY", "") or "")
    openai_api_base: str = field(default_factory=lambda: _env("OPENAI_API_BASE", "") or "")
    openai_api_version: str = field(default_factory=lambda: _env("OPENAI_API_VERSION", "") or "")
    openai_embedding_model: str = field(
        default_factory=lambda: _env("OPENAI_EMBEDDING_MODEL", "text-embedding-ada-002")
        or "text-embedding-ada-002"
    )
    poll_interval_seconds: int = field(default_factory=lambda: _env_int("POLL_INTERVAL_SECONDS", 5))
    max_messages: int = field(default_factory=lambda: _env_int("MAX_MESSAGES", 1))
    visibility_timeout: int = field(default_factory=lambda: _env_int("SQS_VISIBILITY_TIMEOUT", 30))

    @property
    def sqs_queue_url(self) -> str:
        """Return the effective SQS queue URL, preferring LocalStack in dev."""
        if self.app_env == "development" and self.localstack_url:
            queue_name = self.aws_sqs_queue_name.strip() or _queue_name_from_url(self.aws_sqs_queue_url)
            if queue_name:
                base = self.localstack_url.rstrip("/")
                return f"{base}/{_LOCALSTACK_ACCOUNT_ID}/{queue_name}"

        return self.aws_sqs_queue_url

    @property
    def sqs_endpoint_url(self) -> Optional[str]:
        if self.app_env == "development" and self.localstack_url:
            return self.localstack_url.rstrip("/")
        return None

    def validate(self) -> None:
        """Ensure all required settings exist before running."""
        missing = [name for name in ("sqs_queue_url", "db_user", "db_password") if not getattr(self, name)]
        if missing:
            raise RuntimeError(f"Missing required configuration: {', '.join(missing)}")
