# stripe-webhook-idempotency

A Stripe Checkout webhook on AWS Lambda that creates each order once and queues one print job, however the events arrive: retried, duplicated, out of order or at the same time, within the limits listed below.

Stripe's docs say an endpoint can receive the same event more than once, and that events are not guaranteed to arrive in order. This sample shows one way to make that safe, with a test for each case.

It is a work sample: Python 3.12, DynamoDB, SQS FIFO and a SAM template, tested against mocked AWS ([moto](https://github.com/getmoto/moto)). It has not been deployed or run with real payments.

## What it handles

| Situation | Result | Test |
|---|---|---|
| Paid Checkout Session | One order, one print job | `test_paid_checkout_creates_one_order_and_one_print_job` |
| The same event delivered twice | Still one order and one print job | `test_same_event_delivered_twice_creates_one_order` |
| Two deliveries of the same event at the same time | One order, one print job | `test_two_deliveries_at_the_same_time_queue_one_print_job` |
| `completed` and `async_payment_succeeded` for one session | One order | `test_two_event_types_for_one_session_create_one_order` |
| `async_payment_succeeded` arriving before `completed` | One order; the late `completed` gets a 200 | `test_async_success_before_completed_still_one_order` |
| Delayed payment method (`payment_status: unpaid`) | No order until `async_payment_succeeded` | `test_delayed_payment_waits_for_async_success` |
| `payment_status: no_payment_required` in payment mode | One order: Stripe's fulfilment guide fulfils every status except `unpaid` | `test_no_payment_required_in_payment_mode_is_an_order` |
| Setup- or subscription-mode session | 200, ignored: not a print order | `test_session_not_in_payment_mode_is_not_an_order` |
| Wrong secret, changed body, old timestamp, malformed header | 400, nothing written | `test_bad_signature_is_rejected_and_nothing_is_written`, `test_signature.py` |
| SQS unavailable | 500 so Stripe retries; the order is kept; the retry queues once | `test_queue_outage_returns_500_and_the_retry_queues_once` |
| Crash after the message is sent, before the status update | A retry within 5 minutes of the first send sends again with the same deduplication id, and SQS FIFO drops the copy. A later retry queues a second message (see Limits) | `test_crash_after_send_does_not_double_queue_on_retry` |
| Event without a session | 400, nothing written (Stripe retries any non-2xx response, and each retry gets the same 400) | `test_paid_event_without_a_session_is_rejected` |
| Other event types | 200, ignored | `test_other_event_types_are_acknowledged_and_ignored` |
| Base64 body from API Gateway | Verified on the raw bytes | `test_base64_body_from_api_gateway_is_verified_on_raw_bytes` |

32 tests in all.

## How it works

1. **Signature first.** [`signature.py`](src/webhook/signature.py) checks Stripe's v1 scheme on the raw request body: HMAC-SHA256 over `"{t}.{body}"`, a constant-time comparison, any matching `v1` accepted while a secret is being rolled, and a 5-minute tolerance against replays. Malformed or non-ASCII headers get a 400, not a crash.
2. **One order per Checkout Session.** [`orders.py`](src/webhook/orders.py) keys the order on the session id, not the event id, so every event about the same payment lands on the same item. A conditional put (`attribute_not_exists`) creates it once. Only payment-mode sessions become orders.
3. **One print job per order.** The order moves from `paid` to `queued` with a conditional update. The SQS FIFO message uses a SHA-256 of the order id as its deduplication id and message group id: SQS caps both at 128 characters, and Stripe says its ids can be up to 255.
4. **Status codes.** 200 for everything handled, 400 for requests that fail the signature check or are malformed, 500 for temporary failures. Stripe retries any non-2xx response, for up to three days in live mode, and the writes above make every retry safe.

## Limits I would tell a client about

- **The FIFO deduplication window is 5 minutes**, counted from the first send. Stripe doesn't publish its retry intervals (in a sandbox it retries three times over a few hours). So if a delivery fails after the send and Stripe's next attempt comes more than 5 minutes later, that attempt sends a second message. The print consumer should also be idempotent on `order_id`, for example with its own conditional write before calling the print API.
- **Stripe's retries end eventually.** Orders left in `paid` need an alarm, or a scheduled sweep that re-queues `paid` orders older than a few minutes.
- **The signing secret is an environment variable** to keep the template short. In production I'd load it from Secrets Manager at cold start.
- **Amounts are stored as Stripe sends them.** A real pipeline should check the session's line items against its own price list before printing.

## Run it

```bash
uv venv -p 3.12 && uv pip install -e ".[dev]"
.venv/bin/pytest -q
.venv/bin/cfn-lint template.yaml
```

Deploying (not done here) would be `sam build && sam deploy --guided`, then registering the `WebhookUrl` output in the Stripe Dashboard as a **Snapshot** event destination for `checkout.session.completed` and `checkout.session.async_payment_succeeded`. Not Thin, the format the Dashboard marks as recommended: thin events carry no `data.object`, so this handler would answer them with 200 and create no order.

## What I would check first in an order pipeline review

- Every handler that writes: what happens if it runs twice, or stops halfway?
- Every queue: is there a dead-letter queue, an alarm on it, and a way to re-drive messages?
- Every external call (payments, AI, PDF rendering, print API): timeouts, retries with backoff, and idempotency keys where the API supports them.
- Every order state: can someone find an order stuck in it, see why, and re-run it safely?
- Tests: do they cover duplicates, partial failures and replays, or only the happy path?

## Layout

```
src/webhook/handler.py     Lambda entry point (API Gateway HTTP API, payload v2)
src/webhook/signature.py   Stripe signature check
src/webhook/orders.py      idempotent order creation and print-job queueing
tests/                     pytest + moto
template.yaml              SAM: function, HTTP API, DynamoDB table, SQS FIFO queue and dead-letter queue
```

MIT licence.
