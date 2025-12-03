"""AWS FIFO queue utility wrappers."""

from typing import Any, Dict, List, Optional

import boto3

from .config import Settings


class FIFOQueue:
    """Minimal SQS FIFO helper that polls and deletes messages."""

    def __init__(self, settings: Settings, logger) -> None:
        self._logger = logger
        self._settings = settings
        self._client = boto3.client("sqs", region_name=settings.aws_region)
        self._queue_url = settings.sqs_queue_url

    def receive_messages(self) -> List[Dict[str, Any]]:
        """Pull messages from the AWS FIFO queue."""
        self._logger.debug("Polling queue %s", self._queue_url)
        response = self._client.receive_message(
            QueueUrl=self._queue_url,
            MaxNumberOfMessages=self._settings.max_messages,
            VisibilityTimeout=self._settings.visibility_timeout,
            WaitTimeSeconds=20,
            MessageAttributeNames=["All"],
        )
        return response.get("Messages", [])

    def delete_message(self, receipt_handle: str) -> None:
        """Remove a processed message from the queue."""
        self._client.delete_message(QueueUrl=self._queue_url, ReceiptHandle=receipt_handle)
        self._logger.debug("Deleted message from %s", self._queue_url)
