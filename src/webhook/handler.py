"""AWS Lambda handler for Stripe Checkout webhooks behind API Gateway (HTTP API, payload v2).

Status codes (Stripe retries any non-2xx response, for up to three days in live mode):
- 400 for requests that fail the signature check or are malformed (bad JSON, no session).
- 500 for temporary failures (DynamoDB or SQS errors); the idempotent writes in
  orders.py make Stripe's retries safe.
- 200 for everything handled, including duplicates and event types we ignore.
"""

from __future__ import annotations

import base64
import json
import logging
import os
from typing import Any

from .orders import PAID_EVENT_TYPES, OrderStore
from .signature import SignatureError, verify

log = logging.getLogger(__name__)
log.setLevel(logging.INFO)

# As in Stripe's fulfilment guide, every payment status except "unpaid" is fulfilled.
# Setup-mode sessions can also be "no_payment_required", so handle() skips any
# session that is not in payment mode before this check.
FULFILLABLE_PAYMENT_STATUSES = frozenset({"paid", "no_payment_required"})

_store: OrderStore | None = None


def _default_store() -> OrderStore:
    # Created once per Lambda container and reused across invocations.
    global _store
    if _store is None:
        _store = OrderStore(os.environ["ORDERS_TABLE"], os.environ["PRINT_QUEUE_URL"])
    return _store


def _response(status: int, body: dict) -> dict:
    return {
        "statusCode": status,
        "headers": {"content-type": "application/json"},
        "body": json.dumps(body),
    }


def _log(**fields: Any) -> None:
    # Ids and outcomes only: no card, email or address data in the logs.
    log.info(json.dumps(fields, sort_keys=True))


def handle(event: dict, store: OrderStore | None = None, secret: str | None = None) -> dict:
    body = event.get("body") or ""
    payload = base64.b64decode(body) if event.get("isBase64Encoded") else body.encode("utf-8")
    headers = {key.lower(): value for key, value in (event.get("headers") or {}).items()}

    try:
        verify(payload, headers.get("stripe-signature"), secret or os.environ["STRIPE_WEBHOOK_SECRET"])
    except SignatureError as err:
        _log(outcome="rejected", reason=str(err))
        return _response(400, {"error": "invalid signature"})

    try:
        stripe_event = json.loads(payload)
        event_id = stripe_event["id"]
        event_type = stripe_event["type"]
    except (ValueError, KeyError, TypeError):
        _log(outcome="rejected", reason="invalid payload")
        return _response(400, {"error": "invalid payload"})

    if event_type not in PAID_EVENT_TYPES:
        _log(event_id=event_id, type=event_type, outcome="ignored")
        return _response(200, {"received": True})

    session = (stripe_event.get("data") or {}).get("object") or {}
    if not isinstance(session, dict) or not session.get("id"):
        _log(event_id=event_id, type=event_type, outcome="rejected", reason="no checkout session")
        return _response(400, {"error": "invalid payload"})

    if session.get("mode") != "payment":
        _log(event_id=event_id, type=event_type, session=session["id"], outcome="ignored", reason="not payment mode")
        return _response(200, {"received": True})

    if session.get("payment_status") not in FULFILLABLE_PAYMENT_STATUSES:
        # Delayed payment methods complete unpaid; the order is created when
        # checkout.session.async_payment_succeeded arrives.
        _log(event_id=event_id, type=event_type, session=session.get("id"), outcome="awaiting-payment")
        return _response(200, {"received": True})

    store = store or _default_store()
    try:
        order = store.record_paid_session(session, event_id)
        print_job = store.enqueue_print_job(session["id"])
    except Exception:  # temporary failure: let Stripe retry; the writes above are idempotent
        log.exception(json.dumps({"event_id": event_id, "type": event_type, "outcome": "retry"}))
        return _response(500, {"error": "temporary failure"})

    _log(event_id=event_id, type=event_type, order_id=session["id"], order=order, print_job=print_job)
    return _response(200, {"received": True, "order": order, "print_job": print_job})


def lambda_handler(event: dict, context: Any) -> dict:
    return handle(event)
