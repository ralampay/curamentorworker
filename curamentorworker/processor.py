"""Workload processor that downloads S3 payloads, vectorizes them, and persists metadata."""

import json
import os
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

import boto3
import openai
import psycopg
from llama_cpp import Llama
from pdfminer.high_level import extract_text as pdfminer_extract_text

try:
    from langchain.document_loaders import PyPDFLoader
except ModuleNotFoundError:  # pragma: no cover - optional dependency
    PyPDFLoader = None

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
        bucket = payload.get("bucket") or self._settings.s3_bucket_name
        publication_id = payload.get("publication_id")
        key = payload["key"]
        if not bucket or not key:
            self._logger.error("Missing bucket or key in payload %s", payload)
            return

        if self._vector_exists(key):
            self._logger.info("Skipping %s because it already has a vector", key)
            return

        self._logger.info("Processing %s/%s", bucket, key)
        local_path = self._download(bucket, key)
        chunk_payloads = self._vectorize(local_path)
        if not chunk_payloads:
            self._logger.warning("No chunks generated for %s", key)
            os.remove(local_path)
            return
        self._persist_document(key, payload, chunk_payloads, publication_id)
        os.remove(local_path)
        self._logger.info("Cleaned up %s", local_path)

    def _download(self, bucket: str, key: str) -> str:
        dest = tempfile.NamedTemporaryFile(delete=False, suffix=os.path.splitext(key)[1] or ".bin")
        dest.close()
        self._logger.info("Downloading s3://%s/%s to %s", bucket, key, dest.name)
        self._s3.download_file(bucket, key, dest.name)
        return dest.name

    def _ensure_pdf(self, filepath: str) -> None:
        """Reject non-PDF files early by checking the file signature."""
        with open(filepath, "rb") as handle:
            header = handle.read(5)
        if not header.startswith(b"%PDF-"):
            raise ValueError("curamentorworker only supports PDF assets; got %r" % filepath)

    def _vectorize(self, filepath: str) -> List[Dict[str, Any]]:
        """Generate embeddings using llama-cpp-python."""
        self._ensure_pdf(filepath)
        if self._use_local_embeddings:
            self._logger.info(
                "Vectorizing %s with llama-cpp model %s",
                filepath,
                self._settings.llama_model_path,
            )
        else:
            self._logger.info(
                "Vectorizing %s with OpenAI embedding %s",
                filepath,
                self._settings.openai_embedding_model,
            )
        text = self._extract_text_from_pdf(filepath)
        chunks = self._chunk_text(text)
        payloads: List[Dict[str, Any]] = []
        if not chunks:
            return payloads
        if self._use_local_embeddings:
            for index, chunk in enumerate(chunks, start=1):
                response = self._model.embed(chunk)
                payloads.append(
                    {
                        "chunk_index": index,
                        "chunk_text": self._sanitize_text(chunk),
                        "embedding": self._extract_embedding(response),
                    }
                )
        else:
            self._logger.info("Calling OpenAI embeddings endpoint %s", self._settings.openai_embedding_model)
            for index, chunk in enumerate(chunks, start=1):
                self._logger.info("Sending chunk %s/%s to OpenAI embeddings", index, len(chunks))
                chunk_response = self._openai_client.embeddings.create(
                    model=self._settings.openai_embedding_model,
                    input=chunk,
                )
                payloads.append(
                    {
                        "chunk_index": index,
                        "chunk_text": self._sanitize_text(chunk),
                        "embedding": self._extract_embedding(chunk_response),
                    }
                )
        if payloads:
            self._logger.info("Generated %s chunk embeddings for %s", len(payloads), filepath)
        return payloads

    def _chunk_text(self, text: str, chunk_size: int = _EMBEDDING_CHUNK_SIZE) -> List[str]:
        """Split text into fixed-size blocks for OpenAI embeddings."""
        if not text:
            return []
        return [text[i : i + chunk_size] for i in range(0, len(text), chunk_size)]

    def _sanitize_text(self, text: str) -> str:
        """Strip characters that cannot be stored in text fields."""
        return text.replace("\x00", "")

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

    def _extract_text_from_pdf(self, filepath: str) -> str:
        """Return normalized text extracted from a PDF."""
        if PyPDFLoader:
            try:
                loader = PyPDFLoader(filepath)
                documents = loader.load()
                if documents:
                    return "\n".join(doc.page_content for doc in documents if doc.page_content)
                return ""
            except Exception as exc:
                self._logger.exception("LangChain PDF parsing failed for %s: %s", filepath, exc)
        self._logger.info("Falling back to pdfminer for %s", filepath)
        try:
            return pdfminer_extract_text(filepath) or ""
        except Exception as exc:
            self._logger.exception("pdfminer extraction failed for %s: %s", filepath, exc)
            raise

    def create_vector_payload(
        self,
        key: str,
        metadata: Dict[str, Any],
        filepath: str,
        publication_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Return the embedding and metadata that would be inserted for a document."""
        self._logger.info("Creating test vector payload for %s", filepath)
        chunk_payloads = self._vectorize(filepath)

        payload_publication_id = publication_id or metadata.get("publication_id")
        return {
            "key": key,
            "metadata": metadata,
            "chunks": chunk_payloads,
            "publication_id": payload_publication_id,
        }

    def _persist_document(
        self,
        key: str,
        metadata: Dict[str, Any],
        chunk_payloads: Sequence[Dict[str, Any]],
        publication_id: Optional[str],
    ) -> None:
        """Persist vector metadata to PostgreSQL."""
        self._logger.info("Persisting document %s to the database", key)
        now = datetime.utcnow()
        metadata_payload = self._sanitize_text(json.dumps(metadata))
        with psycopg.connect(
            host=self._settings.db_host,
            port=self._settings.db_port,
            dbname=self._settings.db_name,
            user=self._settings.db_user,
            password=self._settings.db_password,
        ) as conn:
            with conn.cursor() as cur:
                for chunk in chunk_payloads:
                    cur.execute(
                        """
                        INSERT INTO publication_vectors (key, publication_id, metadata, vector, chunk_index, chunk_text, created_at, updated_at)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                        (
                            key,
                            publication_id,
                            metadata_payload,
                            chunk["embedding"],
                            chunk["chunk_index"],
                            self._sanitize_text(chunk["chunk_text"]),
                            now,
                            now,
                        ),
                    )
        self._logger.info("Persisted %s to PostgreSQL", key)

    def _vector_exists(self, key: str) -> bool:
        with psycopg.connect(
            host=self._settings.db_host,
            port=self._settings.db_port,
            dbname=self._settings.db_name,
            user=self._settings.db_user,
            password=self._settings.db_password,
        ) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1 FROM publication_vectors WHERE key = %s", (key,))
                return cur.fetchone() is not None
