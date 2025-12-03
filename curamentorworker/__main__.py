"""Entry point for the curamentorworker CLI."""

import time

from .config import Settings
from .logger import get_logger
from .processor import VectorizationProcessor
from .queue import FIFOQueue


def main() -> None:
    """Run the long-lived worker loop."""

    settings = Settings()
    settings.validate()
    logger = get_logger(settings=settings)
    processor = VectorizationProcessor(settings=settings, logger=logger)
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
                try:
                    processor.process_message(message)
                except Exception:  # noqa: BLE001
                    logger.exception("Failed to process message %s", msg_id)
                finally:
                    receipt_handle = message.get("ReceiptHandle")
                    if receipt_handle:
                        queue.delete_message(receipt_handle)

    except KeyboardInterrupt:
        logger.info("Interrupted; exiting gracefully")
