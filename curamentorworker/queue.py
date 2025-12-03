"""AWS FIFO queue utility wrappers."""

from typing import Any, Dict, List, Optional

import boto3

from .config import Settings


class FIFOQueue:
    """Minimal SQS FIFO helper that polls and deletes messages."""

    def __init__(self, settings: Settings, logger) -> None:
        self._logger = logger
        self._settings = settings
        client_kwargs = {"region_name": settings.aws_region}
        if settings.sqs_endpoint_url:
            client_kwargs["endpoint_url"] = settings.sqs_endpoint_url
        self._client = boto3.client("sqs", **client_kwargs)
        self._queue_url = settings.sqs_queue_url

    def receive_messages(self) -> List[Dict[str, Any]]:
        """Pull messages from the AWS FIFO queue."""
        self._logger.info("Polling queue %s", self._queue_url)
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
        try:
            self._client.delete_message(QueueUrl=self._queue_url, ReceiptHandle=receipt_handle)
        except self._client.exceptions.InvalidParameterValue as exc:
            self._logger.info("DeleteMessage got expired receipt: %s", exc)
        else:
            self._logger.info("Deleted message from %s", self._queue_url)

    def extend_visibility(self, receipt_handle: str, visibility_timeout: int) -> None:
        """Extend the visibility timeout for a message while it is being processed."""
        try:
            self._client.change_message_visibility(
                QueueUrl=self._queue_url,
                ReceiptHandle=receipt_handle,
                VisibilityTimeout=visibility_timeout,
            )
        except self._client.exceptions.InvalidParameterValue as exc:
            self._logger.info("change_message_visibility failed (likely expired): %s", exc)
        else:
            self._logger.info(
                "Extended visibility for message to %s seconds on %s",
                visibility_timeout,
                self._queue_url,
            )
