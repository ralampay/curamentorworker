"""Workload processor that downloads S3 payloads, vectorizes them, and persists metadata."""

import json
import os
import tempfile
from typing import Any, Dict

import boto3
import psycopg
from llama_cpp import Llama

from .config import Settings


class VectorizationProcessor:
    """Encapsulates the data flow from S3 → llama → PostgreSQL."""

    def __init__(self, settings: Settings, logger) -> None:
        self._settings = settings
        self._logger = logger
        self._model = Llama(model_path=settings.llama_model_path)
        self._s3 = boto3.client("s3", region_name=settings.aws_region)

    def process_message(self, raw_message: Dict[str, Any]) -> None:
        """Main entry-point for an SQS message."""
        payload = json.loads(raw_message["Body"])
        bucket = payload.get("bucket") or self._settings.s3_bucket_name
        key = payload["key"]
        if not bucket or not key:
            self._logger.error("Missing bucket or key in payload %s", payload)
            return

        self._logger.info("Processing %s/%s", bucket, key)
        local_path = self._download(bucket, key)
        vector = self._vectorize(local_path)
        self._persist_document(key, payload, vector)
        os.remove(local_path)
        self._logger.debug("Cleaned up %s", local_path)

    def _download(self, bucket: str, key: str) -> str:
        dest = tempfile.NamedTemporaryFile(delete=False, suffix=os.path.splitext(key)[1] or ".bin")
        dest.close()
        self._logger.debug("Downloading s3://%s/%s to %s", bucket, key, dest.name)
        self._s3.download_file(bucket, key, dest.name)
        return dest.name

    def _vectorize(self, filepath: str) -> Dict[str, Any]:
        """Generate embeddings using llama-cpp-python."""
        self._logger.debug("Vectorizing %s with llama-cpp model %s", filepath, self._settings.llama_model_path)
        with open(filepath, "r", encoding="utf-8", errors="ignore") as handle:
            text = handle.read()
        response = self._model.embed(text)
        self._logger.debug("Generated embedding of length %s", len(response["data"][0]["embedding"]))
        return response

    def _persist_document(self, key: str, metadata: Dict[str, Any], vector_response: Dict[str, Any]) -> None:
        """Persist vector metadata to PostgreSQL."""
        self._logger.debug("Persisting document %s to the database", key)
        vector = vector_response["data"][0]["embedding"]
        with psycopg.connect(
            host=self._settings.db_host,
            port=self._settings.db_port,
            dbname=self._settings.db_name,
            user=self._settings.db_user,
            password=self._settings.db_password,
        ) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO vectorized_documents (s3_key, metadata, vector)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (s3_key) DO UPDATE
                    SET metadata = EXCLUDED.metadata, vector = EXCLUDED.vector""",
                    (key, json.dumps(metadata), vector),
                )
        self._logger.info("Persisted %s to PostgreSQL", key)
