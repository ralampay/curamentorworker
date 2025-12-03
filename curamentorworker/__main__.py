"""Entry point for the curamentorworker CLI."""

import argparse
import os
import time
from pathlib import Path
from typing import Dict, List, Optional

from dotenv import load_dotenv

from .config import Settings
from .logger import get_logger
from .processor import VectorizationProcessor
from .queue import FIFOQueue


def main() -> None:
    """Run the long-lived worker loop."""

    dotenv_loaded = load_dotenv(Path(".env"))
    args = _parse_args()
    settings = Settings()
    logger = get_logger(settings=settings)

    if dotenv_loaded:
        logger.info("Loaded environment variables from %s", Path(".env").resolve())
    else:
        logger.debug("No .env file found at %s", Path(".env").resolve())

    if args.test_vectorize:
        run_test_vectorize(
            filepath=args.test_vectorize,
            key=args.test_key,
            metadata_pairs=args.test_metadata,
            settings=settings,
            logger=logger,
            use_local=args.local,
        )
        return

    settings.validate()
    processor = VectorizationProcessor(settings=settings, logger=logger, use_local_embeddings=args.local)
    queue = FIFOQueue(settings=settings, logger=logger)

    logger.info("Starting curamentorworker in %s mode", settings.app_env)

    try:
        while True:
            messages = queue.receive_messages()
            if not messages:
                logger.debug("No messages in queue; sleeping for %s seconds", settings.poll_interval_seconds)
                time.sleep(settings.poll_interval_seconds)
                continue

            for message in messages:
                msg_id = message.get("MessageId", "unknown")
                logger.info("Handling message %s", msg_id)
                processed = False
                try:
                    processor.process_message(message)
                    processed = True
                except Exception:  # noqa: BLE001
                    logger.exception("Failed to process message %s", msg_id)
                finally:
                    receipt_handle = message.get("ReceiptHandle")
                    if processed and receipt_handle:
                        queue.delete_message(receipt_handle)

    except KeyboardInterrupt:
        logger.info("Interrupted; exiting gracefully")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the curamentorworker CLI.")
    parser.add_argument(
        "--test-vectorize",
        metavar="FILE",
        help="Vectorize a local file with the configured model and show the metadata that would be inserted.",
    )
    parser.add_argument(
        "--test-key",
        help="Override the document key used in --test-vectorize (defaults to the file name).",
    )
    parser.add_argument(
        "--test-metadata",
        metavar="KEY=VALUE",
        action="append",
        default=[],
        help="Add metadata to include with the test vector payload (can be repeated).",
    )
    parser.add_argument(
        "--local",
        action="store_true",
        help="Force embeddings to run locally via llama-cpp-python and LLAMA_MODEL_PATH rather than OpenAI.",
    )
    return parser.parse_args()


def _parse_metadata(pairs: List[str]) -> Dict[str, str]:
    metadata = {}
    for pair in pairs:
        if "=" not in pair:
            raise ValueError("metadata entries must be in KEY=VALUE form")
        key, value = pair.split("=", 1)
        metadata[key.strip()] = value.strip()
    return metadata


def run_test_vectorize(
    filepath: str,
    key: Optional[str],
    metadata_pairs: List[str],
    settings: Settings,
    logger,
    use_local: bool,
) -> None:
    """Vectorize a local file and log the payload that would be persisted."""
    processor = VectorizationProcessor(settings=settings, logger=logger, use_local_embeddings=use_local)

    if not os.path.isfile(filepath):
        logger.error("Test file %s does not exist", filepath)
        raise SystemExit(1)

    metadata = {"source": "test-vectorize"}
    if metadata_pairs:
        try:
            metadata.update(_parse_metadata(metadata_pairs))
        except ValueError as exc:
            logger.error("Failed to parse metadata for --test-vectorize: %s", exc)
            raise SystemExit(1) from exc

    document_key = key or os.path.basename(filepath)
    payload = processor.create_vector_payload(document_key, metadata, filepath)
    logger.info(
        "Test vector ready for %s (%s metadata entries, vector length %s)",
        payload["s3_key"],
        len(payload["metadata"]),
        len(payload["vector"]),
    )
    logger.debug("Vector metadata payload: %s", payload)


if __name__ == "__main__":
    main()
