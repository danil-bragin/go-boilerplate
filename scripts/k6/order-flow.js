// k6 load test — order creation flow against the gateway.
//
// Flow per iteration:
//   1. POST /v1/orders with a unique Idempotency-Key and an amount BELOW the
//      payments decline threshold (1_000_000 cents) so the choreography ends
//      in "paid", not "payment_failed".
//   2. Poll GET /v1/orders/{id} until the projection row reaches a settled
//      status ("created" or "paid") or the poll budget is exhausted.
//
// Environment:
//   BASE_URL — gateway base URL (default http://localhost:8080). When k6 runs
//              inside Docker, localhost is the CONTAINER, not your machine —
//              use http://host.docker.internal:8080 (see justfile `load`).
//   TOKEN    — optional Bearer token (e.g. `just token`). When set, the JWT's
//              `sub` claim is used as customer_id so that GET /v1/orders/{id}
//              passes the read-path ownership check for non-admin principals.
//
// Run (local k6):   k6 run --vus 10 --duration 30s scripts/k6/order-flow.js
// Run (dockerized): just load [vus] [duration]
//
// Expected output against a running stack (`just up-apps`): all checks pass,
// http_req_duration p(99) < 500ms. Against an ABSENT server every request
// fails to connect, the `checks` threshold trips, and k6 exits non-zero —
// that is the script working as designed, not a script bug.

import http from 'k6/http';
import { check, sleep } from 'k6';
import encoding from 'k6/encoding';

const BASE_URL = __ENV.BASE_URL || 'http://localhost:8080';
const TOKEN = __ENV.TOKEN || '';

export const options = {
  thresholds: {
    // p99 latency budget for ALL http requests issued by this script.
    http_req_duration: ['p(99)<500'],
    // <1% of checks may fail (connection errors, non-2xx, never-settled polls).
    checks: ['rate>0.99'],
  },
};

// jwtSub extracts the `sub` claim from a JWT without verifying it (the
// gateway verifies; we only need the subject for the ownership check).
function jwtSub(token) {
  try {
    const payload = token.split('.')[1];
    return JSON.parse(encoding.b64decode(payload, 'rawurl', 's')).sub || '';
  } catch (e) {
    return '';
  }
}

const CUSTOMER_ID = TOKEN ? jwtSub(TOKEN) || 'k6-customer' : 'k6-customer';

function headers(extra) {
  const h = Object.assign({ 'Content-Type': 'application/json' }, extra || {});
  if (TOKEN) {
    h['Authorization'] = `Bearer ${TOKEN}`;
  }
  return h;
}

export default function () {
  // Unique per iteration: VU id + iteration counter + timestamp. The gateway
  // derives the order id deterministically (UUIDv5) from this key, so reusing
  // a key would dedupe instead of creating load.
  const idempotencyKey = `k6-${__VU}-${__ITER}-${Date.now()}`;

  const body = JSON.stringify({
    customer_id: CUSTOMER_ID,
    amount_cents: 1234, // below the payments decline threshold (1_000_000)
    currency: 'USD',
  });

  const res = http.post(`${BASE_URL}/v1/orders`, body, {
    headers: headers({ 'Idempotency-Key': idempotencyKey }),
  });

  const accepted = check(res, {
    'POST /v1/orders is 202': (r) => r.status === 202,
  });
  if (!accepted) {
    sleep(1);
    return;
  }

  const orderId = res.json('order_id');

  // Poll the read model until the projection settles. "created" appears once
  // the OrderCreated event is projected; "paid" once the payment settles.
  let settled = false;
  for (let i = 0; i < 20; i++) {
    sleep(0.5);
    const get = http.get(`${BASE_URL}/v1/orders/${orderId}`, { headers: headers() });
    if (get.status === 200) {
      const status = get.json('status');
      if (status === 'created' || status === 'paid') {
        settled = true;
        break;
      }
    }
    // 404 just means the projection has not caught up yet — keep polling.
  }

  check(null, {
    'order reached created/paid within poll budget': () => settled,
  });
}
