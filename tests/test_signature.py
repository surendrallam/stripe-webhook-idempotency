import hashlib
import hmac
import time

import pytest

from conftest import SECRET, sign
from webhook.signature import SignatureError, verify

PAYLOAD = b'{"id":"evt_1","type":"checkout.session.completed"}'


def test_valid_signature_passes():
    verify(PAYLOAD, sign(PAYLOAD), SECRET)


def test_wrong_secret_fails():
    with pytest.raises(SignatureError, match="no matching"):
        verify(PAYLOAD, sign(PAYLOAD, secret="whsec_other"), SECRET)


def test_changed_body_fails():
    header = sign(PAYLOAD)
    with pytest.raises(SignatureError, match="no matching"):
        verify(PAYLOAD.replace(b"evt_1", b"evt_2"), header, SECRET)


def test_old_timestamp_fails():
    signed_at = int(time.time()) - 301
    with pytest.raises(SignatureError, match="older than"):
        verify(PAYLOAD, sign(PAYLOAD, timestamp=signed_at), SECRET)


def test_timestamp_within_tolerance_passes():
    verify(PAYLOAD, sign(PAYLOAD, timestamp=int(time.time()) - 299), SECRET)


def test_any_matching_v1_passes_during_secret_roll():
    t = int(time.time())
    good = hmac.new(SECRET.encode(), f"{t}.".encode() + PAYLOAD, hashlib.sha256).hexdigest()
    verify(PAYLOAD, f"t={t},v1={'0' * 64},v1={good}", SECRET)


def test_v0_alone_is_not_accepted():
    t = int(time.time())
    digest = hmac.new(SECRET.encode(), f"{t}.".encode() + PAYLOAD, hashlib.sha256).hexdigest()
    with pytest.raises(SignatureError, match="malformed"):
        verify(PAYLOAD, f"t={t},v0={digest}", SECRET)


@pytest.mark.parametrize(
    "header", [None, "", "garbage", "t=abc,v1=00", "v1=00", "t=²,v1=00", "t=1,v1=é"]
)
def test_missing_or_malformed_header_fails(header):
    with pytest.raises(SignatureError):
        verify(PAYLOAD, header, SECRET)
