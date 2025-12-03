"""Workload processor that downloads S3 payloads, vectorizes them, and persists metadata."""

import json
import os
import tempfile
from pathlib import Path
from typing import Any, Dict, List, Optional

import boto3
import openai
import psycopg
from llama_cpp import Llama

from .config import Settings

_EMBEDDING_CHUNK_SIZE = 4000


class VectorizationProcessor:
    """Encapsulates the data flow from S3 → llama → PostgreSQL."""

    def __init__(self, settings: Settings, logger, use_local_embeddings: bool) -> None:
        self._settings = settings
        self._logger = logger
        self._use_local_embeddings = use_local_embeddings
        self._model: Optional[Llama] = None
        self._openai_client = None
        if use_local_embeddings:
            self._model = Llama(model_path=settings.llama_model_path)
        else:
            if not settings.openai_api_key:
                raise RuntimeError("OPENAI_API_KEY is required for remote embedding mode")
            client_kwargs: Dict[str, Any] = {"api_key": settings.openai_api_key}
            if settings.openai_api_base:
                client_kwargs["base_url"] = settings.openai_api_base
            if settings.openai_api_version:
                client_kwargs["api_version"] = settings.openai_api_version
            self._openai_client = openai.OpenAI(**client_kwargs)
        self._s3 = boto3.client("s3", region_name=settings.aws_region)

    def process_message(self, raw_message: Dict[str, Any]) -> None:
        """Main entry-point for an SQS message."""
        payload = json.loads(raw_message["Body"])
        print(payload)
        bucket = payload.get("bucket") or self._settings.s3_bucket_name
        publication_id = payload.get("publication_id")
        key = payload["key"]
        if not bucket or not key:
            self._logger.error("Missing bucket or key in payload %s", payload)
            return

        self._logger.info("Processing %s/%s", bucket, key)
        local_path = self._download(bucket, key)
        vector_response = self._vectorize(local_path)
        self._persist_document(key, payload, vector_response, publication_id)
        os.remove(local_path)
        self._logger.debug("Cleaned up %s", local_path)

    def _download(self, bucket: str, key: str) -> str:
        dest = tempfile.NamedTemporaryFile(delete=False, suffix=os.path.splitext(key)[1] or ".bin")
        dest.close()
        self._logger.debug("Downloading s3://%s/%s to %s", bucket, key, dest.name)
        self._s3.download_file(bucket, key, dest.name)
        return dest.name

    def _ensure_pdf(self, filepath: str) -> None:
        """Reject non-PDF files early by checking the file signature."""
        with open(filepath, "rb") as handle:
            header = handle.read(5)
        if not header.startswith(b"%PDF-"):
            raise ValueError("curamentorworker only supports PDF assets; got %r" % filepath)

    def _vectorize(self, filepath: str) -> Dict[str, Any]:
        """Generate embeddings using llama-cpp-python."""
        self._ensure_pdf(filepath)
        if self._use_local_embeddings:
            self._logger.debug(
                "Vectorizing %s with llama-cpp model %s",
                filepath,
                self._settings.llama_model_path,
            )
        else:
            self._logger.debug(
                "Vectorizing %s with OpenAI embedding %s",
                filepath,
                self._settings.openai_embedding_model,
            )
        text = self._read_text(filepath)
        if self._use_local_embeddings:
            response = self._model.embed(text)
        else:
            self._logger.debug("Calling OpenAI embeddings endpoint %s", self._settings.openai_embedding_model)
            chunks = self._chunk_text(text)
            if len(chunks) == 1:
                response = self._openai_client.embeddings.create(
                    model=self._settings.openai_embedding_model,
                    input=chunks[0],
                )
            else:
                chunk_embeddings: List[List[float]] = []
                for index, chunk in enumerate(chunks, start=1):
                    self._logger.debug("Sending chunk %s/%s to OpenAI embeddings", index, len(chunks))
                    chunk_response = self._openai_client.embeddings.create(
                        model=self._settings.openai_embedding_model,
                        input=chunk,
                    )
                    chunk_embeddings.append(chunk_response.data[0].embedding)
                averaged = self._average_embeddings(chunk_embeddings)
                self._logger.debug("Averaged %s chunk embeddings for %s", len(chunk_embeddings), filepath)
                response = {"data": [{"embedding": averaged}]}
        embedding_vector = self._extract_embedding(response)
        self._logger.debug("Generated embedding of length %s", len(embedding_vector))
        return response

    def _chunk_text(self, text: str, chunk_size: int = _EMBEDDING_CHUNK_SIZE) -> List[str]:
        """Split text into fixed-size blocks for OpenAI embeddings."""
        if not text:
            return []
        return [text[i : i + chunk_size] for i in range(0, len(text), chunk_size)]

    def _average_embeddings(self, embeddings: List[List[float]]) -> List[float]:
        if not embeddings:
            return []
        length = len(embeddings[0])
        totals = [0.0] * length
        for vector in embeddings:
            if len(vector) != length:
                raise ValueError("Mismatched embedding lengths when averaging")
            for idx, value in enumerate(vector):
                totals[idx] += value
        count = len(embeddings)
        return [value / count for value in totals]

    def _extract_embedding(self, response: Any) -> List[float]:
        if hasattr(response, "data"):
            return response.data[0].embedding
        return response["data"][0]["embedding"]

    def _read_text(self, filepath: str) -> str:
        with open(filepath, "r", encoding="utf-8", errors="ignore") as handle:
            return handle.read()

    def create_vector_payload(
        self,
        key: str,
        metadata: Dict[str, Any],
        filepath: str,
        publication_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Return the embedding and metadata that would be inserted for a document."""
        self._logger.debug("Creating test vector payload for %s", filepath)
        vector_response = self._vectorize(filepath)

        payload_publication_id = publication_id or metadata.get("publication_id")
        return {
            "s3_key": key,
            "metadata": metadata,
            "vector": vector_response["data"][0]["embedding"],
            "publication_id": payload_publication_id,
        }

    def _persist_document(
        self,
        key: str,
        metadata: Dict[str, Any],
        vector_response: Dict[str, Any],
        publication_id: Optional[str],
    ) -> None:
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
                    INSERT INTO publication_vectors (s3_key, publication_id, metadata, vector)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (s3_key) DO UPDATE
                    SET publication_id = EXCLUDED.publication_id,
                        metadata = EXCLUDED.metadata,
                        vector = EXCLUDED.vector""",
                    (key, publication_id, json.dumps(metadata), vector),
                )
        self._logger.info("Persisted %s to PostgreSQL", key)
