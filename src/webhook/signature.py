"""Verify Stripe webhook signatures with Stripe's documented v1 scheme.

The Stripe-Signature header looks like ``t=1700000000,v1=<hex>,v1=<hex>``.
The signature is HMAC-SHA256 over ``"{t}." + raw_body`` with the endpoint's
signing secret. There can be several v1 values while a secret is being rolled.
In a codebase that already depends on stripe-python, ``stripe.Webhook.construct_event``
does the same check.
"""

from __future__ import annotations

import hashlib
import hmac
import time

# Stripe's libraries reject events signed more than 5 minutes ago by default.
DEFAULT_TOLERANCE_SECONDS = 300


class SignatureError(Exception):
    """The request did not come from Stripe, was changed, or is too old."""


def verify(
    payload: bytes,
    header: str | None,
    secret: str,
    tolerance: int = DEFAULT_TOLERANCE_SECONDS,
    now: float | None = None,
) -> None:
    """Raise SignatureError unless ``header`` is a valid signature of ``payload``.

    ``payload`` must be the raw request body exactly as received: parsing and
    re-serialising the JSON changes the bytes and breaks the signature.
    """
    if not header:
        raise SignatureError("missing Stripe-Signature header")

    timestamp: str | None = None
    signatures: list[str] = []
    for part in header.split(","):
        key, _, value = part.strip().partition("=")
        if key == "t":
            timestamp = value
        elif key == "v1" and value.isascii():
            signatures.append(value)

    # isascii() first: str.isdigit() also accepts characters such as "²".
    if timestamp is None or not (timestamp.isascii() and timestamp.isdigit()) or not signatures:
        raise SignatureError("malformed Stripe-Signature header")

    signed_payload = timestamp.encode("ascii") + b"." + payload
    expected = hmac.new(secret.encode("utf-8"), signed_payload, hashlib.sha256).hexdigest()
    if not any(hmac.compare_digest(expected, candidate) for candidate in signatures):
        raise SignatureError("no matching v1 signature")

    current = time.time() if now is None else now
    if current - int(timestamp) > tolerance:
        raise SignatureError("signature timestamp is older than the tolerance")
