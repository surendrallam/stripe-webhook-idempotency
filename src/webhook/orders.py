"""Order creation that stays correct under repeated, concurrent and out-of-order deliveries.

Two rules carry the design:

1. One order per Checkout Session. The order key is the session id, so a retried
   event, a duplicate event and a second event type for the same payment all land
   on the same DynamoDB item. A conditional put creates it once.
2. The print job is sent at most once per order in normal operation. The order
   moves from ``paid`` to ``queued`` with a conditional update, and the SQS FIFO
   message uses a SHA-256 of the order id as its deduplication id. README.md lists
   the limits.
"""

from __future__ import annotations

import hashlib
import json
import time
from typing import Any

import boto3
from botocore.exceptions import ClientError

# A Checkout Session is paid when it completes with payment_status "paid", or later,
# for delayed payment methods, when async_payment_succeeded arrives.
PAID_EVENT_TYPES = frozenset(
    {"checkout.session.completed", "checkout.session.async_payment_succeeded"}
)


def _condition_failed(err: ClientError) -> bool:
    return err.response.get("Error", {}).get("Code") == "ConditionalCheckFailedException"


class OrderStore:
    def __init__(self, table_name: str, queue_url: str, dynamodb: Any = None, sqs: Any = None):
        self.table = (dynamodb or boto3.resource("dynamodb")).Table(table_name)
        self.sqs = sqs or boto3.client("sqs")
        self.queue_url = queue_url

    def record_paid_session(self, session: dict, event_id: str) -> str:
        """Create the order for a paid session once. Returns "created" or "exists"."""
        item = {
            "order_id": session["id"],
            "status": "paid",
            "amount_total": session.get("amount_total"),
            "currency": session.get("currency"),
            "customer": session.get("customer"),
            "first_event_id": event_id,
            "created_at": int(time.time()),
        }
        try:
            self.table.put_item(Item=item, ConditionExpression="attribute_not_exists(order_id)")
        except ClientError as err:
            if _condition_failed(err):
                return "exists"
            raise
        return "created"

    def enqueue_print_job(self, order_id: str) -> str:
        """Send the order to the print queue unless it is already queued."""
        order = self.table.get_item(Key={"order_id": order_id}, ConsistentRead=True).get("Item")
        if order is None:
            raise LookupError(f"order {order_id} not found")
        if order["status"] == "queued":
            return "already-queued"

        # Same order, same deduplication id: SQS FIFO drops a second copy sent within
        # 5 minutes of the first. SQS caps both ids at 128 characters and Stripe says
        # its ids can reach 255, so the queue gets a fixed-length hash of the order id.
        queue_id = hashlib.sha256(order_id.encode("utf-8")).hexdigest()
        self.sqs.send_message(
            QueueUrl=self.queue_url,
            MessageBody=json.dumps({"order_id": order_id}),
            MessageGroupId=queue_id,
            MessageDeduplicationId=queue_id,
        )
        try:
            self.table.update_item(
                Key={"order_id": order_id},
                UpdateExpression="SET #status = :queued, queued_at = :now",
                ConditionExpression="#status = :paid",
                ExpressionAttributeNames={"#status": "status"},
                ExpressionAttributeValues={":queued": "queued", ":paid": "paid", ":now": int(time.time())},
            )
        except ClientError as err:
            if not _condition_failed(err):
                raise
            # A concurrent delivery marked it first; FIFO deduplication dropped our copy.
        return "queued"
