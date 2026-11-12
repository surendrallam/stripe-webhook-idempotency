import base64
import hashlib
import hmac
import json
import time
from types import SimpleNamespace

import boto3
import pytest
from moto import mock_aws

from webhook.orders import OrderStore

SECRET = "whsec_test_secret"
REGION = "us-east-1"


@pytest.fixture(autouse=True)
def aws_env(monkeypatch):
    # Fake credentials so nothing can ever reach a real AWS account.
    monkeypatch.setenv("AWS_DEFAULT_REGION", REGION)
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")


@pytest.fixture
def aws():
    with mock_aws():
        dynamodb = boto3.resource("dynamodb", region_name=REGION)
        dynamodb.create_table(
            TableName="orders",
            KeySchema=[{"AttributeName": "order_id", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "order_id", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST",
        )
        sqs = boto3.client("sqs", region_name=REGION)
        queue_url = sqs.create_queue(
            QueueName="print-jobs.fifo", Attributes={"FifoQueue": "true"}
        )["QueueUrl"]
        yield SimpleNamespace(dynamodb=dynamodb, sqs=sqs, queue_url=queue_url, table=dynamodb.Table("orders"))


@pytest.fixture
def store(aws):
    return OrderStore("orders", aws.queue_url, dynamodb=aws.dynamodb, sqs=aws.sqs)


def sign(payload: bytes, secret: str = SECRET, timestamp: int | None = None) -> str:
    t = int(time.time()) if timestamp is None else timestamp
    digest = hmac.new(secret.encode(), f"{t}.".encode() + payload, hashlib.sha256).hexdigest()
    return f"t={t},v1={digest}"


def checkout_event(
    event_id="evt_1",
    session_id="cs_test_1",
    event_type="checkout.session.completed",
    payment_status="paid",
) -> dict:
    return {
        "id": event_id,
        "object": "event",
        "type": event_type,
        "data": {
            "object": {
                "id": session_id,
                "object": "checkout.session",
                "mode": "payment",
                "payment_status": payment_status,
                "amount_total": 4900,
                "currency": "usd",
                "customer": "cus_test_1",
            }
        },
    }


def api_request(stripe_event: dict, signature: str | None = None, base64_body: bool = False) -> dict:
    """An API Gateway HTTP API (payload v2) request as Lambda receives it."""
    raw = json.dumps(stripe_event).encode()
    headers = {"content-type": "application/json"}
    headers["stripe-signature"] = sign(raw) if signature is None else signature
    return {
        "version": "2.0",
        "headers": headers,
        "body": base64.b64encode(raw).decode() if base64_body else raw.decode(),
        "isBase64Encoded": base64_body,
    }


def messages_in_queue(aws) -> int:
    attrs = aws.sqs.get_queue_attributes(
        QueueUrl=aws.queue_url, AttributeNames=["ApproximateNumberOfMessages"]
    )["Attributes"]
    return int(attrs["ApproximateNumberOfMessages"])


def orders(aws) -> list[dict]:
    return aws.table.scan()["Items"]
