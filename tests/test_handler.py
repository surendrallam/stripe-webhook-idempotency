import json

import pytest
from botocore.exceptions import ClientError

from conftest import SECRET, api_request, checkout_event, messages_in_queue, orders
from webhook.handler import handle
from webhook.orders import OrderStore


def call(store, request):
    response = handle(request, store=store, secret=SECRET)
    return response["statusCode"], json.loads(response["body"])


def test_paid_checkout_creates_one_order_and_one_print_job(aws, store):
    status, body = call(store, api_request(checkout_event()))
    assert status == 200
    assert body == {"received": True, "order": "created", "print_job": "queued"}
    [order] = orders(aws)
    assert order["order_id"] == "cs_test_1" and order["status"] == "queued"
    assert messages_in_queue(aws) == 1


def test_same_event_delivered_twice_creates_one_order(aws, store):
    request = api_request(checkout_event())
    call(store, request)
    status, body = call(store, request)
    assert status == 200
    assert body == {"received": True, "order": "exists", "print_job": "already-queued"}
    assert len(orders(aws)) == 1
    assert messages_in_queue(aws) == 1


def test_two_event_types_for_one_session_create_one_order(aws, store):
    call(store, api_request(checkout_event(event_id="evt_1")))
    call(
        store,
        api_request(checkout_event(event_id="evt_2", event_type="checkout.session.async_payment_succeeded")),
    )
    assert len(orders(aws)) == 1
    assert messages_in_queue(aws) == 1


def test_delayed_payment_waits_for_async_success(aws, store):
    status, _ = call(store, api_request(checkout_event(payment_status="unpaid")))
    assert status == 200
    assert orders(aws) == []

    call(store, api_request(checkout_event(event_id="evt_2", event_type="checkout.session.async_payment_succeeded")))
    assert len(orders(aws)) == 1
    assert messages_in_queue(aws) == 1


def test_no_payment_required_in_payment_mode_is_an_order(aws, store):
    status, body = call(store, api_request(checkout_event(payment_status="no_payment_required")))
    assert status == 200 and body["order"] == "created"
    assert messages_in_queue(aws) == 1


@pytest.mark.parametrize("mode", ["setup", "subscription"])
def test_session_not_in_payment_mode_is_not_an_order(aws, store, mode):
    event = checkout_event(payment_status="no_payment_required")
    event["data"]["object"]["mode"] = mode
    status, body = call(store, api_request(event))
    assert status == 200 and body == {"received": True}
    assert orders(aws) == []
    assert messages_in_queue(aws) == 0


def test_async_success_before_completed_still_one_order(aws, store):
    call(store, api_request(checkout_event(event_id="evt_2", event_type="checkout.session.async_payment_succeeded")))
    status, body = call(store, api_request(checkout_event(event_id="evt_1", payment_status="unpaid")))
    assert status == 200 and body == {"received": True}
    assert len(orders(aws)) == 1
    assert messages_in_queue(aws) == 1


class InterleavingTable:
    """Runs a second delivery between this delivery's read and its send."""

    def __init__(self, real, second_delivery):
        self.real = real
        self.second_delivery = second_delivery

    def __getattr__(self, name):
        return getattr(self.real, name)

    def get_item(self, **kwargs):
        item = self.real.get_item(**kwargs)
        if self.second_delivery:
            run, self.second_delivery = self.second_delivery, None
            run()
        return item


def test_two_deliveries_at_the_same_time_queue_one_print_job(aws, store):
    request = api_request(checkout_event())
    other_container = OrderStore("orders", aws.queue_url, dynamodb=aws.dynamodb, sqs=aws.sqs)
    second = {}
    store.table = InterleavingTable(store.table, lambda: second.update(result=call(other_container, request)))

    status, body = call(store, request)

    assert status == 200 and body["print_job"] == "queued"
    assert second["result"][0] == 200
    assert len(orders(aws)) == 1
    assert messages_in_queue(aws) == 1  # the later send was dropped by FIFO deduplication
    [order] = orders(aws)
    assert order["status"] == "queued"


def test_bad_signature_is_rejected_and_nothing_is_written(aws, store):
    request = api_request(checkout_event(), signature="t=1,v1=" + "0" * 64)
    status, body = call(store, request)
    assert status == 400 and body == {"error": "invalid signature"}
    assert orders(aws) == []
    assert messages_in_queue(aws) == 0


def test_signed_but_invalid_json_is_rejected(aws, store):
    from conftest import sign

    raw = b"not json"
    request = {"headers": {"Stripe-Signature": sign(raw)}, "body": raw.decode(), "isBase64Encoded": False}
    status, body = call(store, request)
    assert status == 400 and body == {"error": "invalid payload"}


def test_other_event_types_are_acknowledged_and_ignored(aws, store):
    status, body = call(store, api_request(checkout_event(event_type="customer.created")))
    assert status == 200 and body == {"received": True}
    assert orders(aws) == []


def test_base64_body_from_api_gateway_is_verified_on_raw_bytes(aws, store):
    status, body = call(store, api_request(checkout_event(), base64_body=True))
    assert status == 200 and body["order"] == "created"


class FailingSqs:
    """Wraps the real client and fails the first send, like an SQS outage."""

    def __init__(self, real, failures=1):
        self.real = real
        self.failures = failures

    def send_message(self, **kwargs):
        if self.failures:
            self.failures -= 1
            raise ClientError({"Error": {"Code": "ServiceUnavailable", "Message": "down"}}, "SendMessage")
        return self.real.send_message(**kwargs)


def test_queue_outage_returns_500_and_the_retry_queues_once(aws, store):
    store.sqs = FailingSqs(aws.sqs)
    request = api_request(checkout_event())

    status, _ = call(store, request)
    assert status == 500  # Stripe will retry
    [order] = orders(aws)
    assert order["status"] == "paid"  # order kept, print job still owed
    assert messages_in_queue(aws) == 0

    status, body = call(store, request)  # Stripe's retry
    assert status == 200
    assert body == {"received": True, "order": "exists", "print_job": "queued"}
    assert messages_in_queue(aws) == 1


class FailingTable:
    """Lets the message go out, then fails the status update once (crash between the two)."""

    def __init__(self, real):
        self.real = real
        self.fail_next_update = True

    def __getattr__(self, name):
        return getattr(self.real, name)

    def update_item(self, **kwargs):
        if self.fail_next_update:
            self.fail_next_update = False
            raise ClientError({"Error": {"Code": "InternalServerError", "Message": "boom"}}, "UpdateItem")
        return self.real.update_item(**kwargs)


def test_crash_after_send_does_not_double_queue_on_retry(aws, store):
    store.table = FailingTable(store.table)
    request = api_request(checkout_event())

    status, _ = call(store, request)
    assert status == 500
    assert messages_in_queue(aws) == 1

    status, body = call(store, request)  # immediate retry, inside SQS's 5-minute window
    assert status == 200 and body["print_job"] == "queued"
    assert messages_in_queue(aws) == 1  # FIFO deduplication dropped the second copy
    [order] = orders(aws)
    assert order["status"] == "queued"


def test_paid_event_without_a_session_is_rejected(aws, store):
    event = checkout_event()
    event["data"] = {}
    status, body = call(store, api_request(event))
    assert status == 400 and body == {"error": "invalid payload"}
    assert orders(aws) == []


@pytest.mark.parametrize("missing", ["id", "type"])
def test_event_without_id_or_type_is_rejected(aws, store, missing):
    event = checkout_event()
    del event[missing]
    status, _ = call(store, api_request(event))
    assert status == 400
