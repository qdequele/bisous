#!/usr/bin/env python3
"""A fake Meilisearch Lab for scripts/e2e.sh: the two internal endpoints
meili-connect calls.

    fake_lab.py <port> <instance id> <instance secret> <workdir>

- POST /internal/events: checks X-Lab-Instance-Id and the signature
  (X-Lab-Signature: sha256=HMAC(secret, "<X-Lab-Timestamp>.<raw body>")),
  answers 401 when either is wrong, appends every event of a good batch to
  <workdir>/lab_events.jsonl (one JSON object per line, deduplicated by id
  as the Lab does) and answers {"accepted": [ids]}.
- GET /internal/accounts/<id>: checks X-Lab-Instance-Id and the bearer
  secret; answers {"active": true, "credits": {"balance": n},
  "can_spend": <bool>} where <bool> is the content of <workdir>/can_spend
  ("false" holds the team; anything else, or no file, lets it spend).

Standard library only.
"""

import hashlib
import hmac
import json
import os
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

PORT, INSTANCE, SECRET, WORK = int(sys.argv[1]), sys.argv[2], sys.argv[3], sys.argv[4]
EVENTS = os.path.join(WORK, "lab_events.jsonl")
CAN_SPEND = os.path.join(WORK, "can_spend")
SEEN = set()


class Lab(BaseHTTPRequestHandler):
    def log_message(self, *_):
        pass

    def answer(self, status, body):
        data = json.dumps(body).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_POST(self):
        if self.path != "/internal/events":
            return self.answer(404, {"error": "not found"})
        raw = self.rfile.read(int(self.headers.get("Content-Length", 0)))
        ts = self.headers.get("X-Lab-Timestamp", "")
        expected = "sha256=" + hmac.new(
            SECRET.encode(), ts.encode() + b"." + raw, hashlib.sha256
        ).hexdigest()
        given = self.headers.get("X-Lab-Signature", "")
        if self.headers.get("X-Lab-Instance-Id") != INSTANCE or not hmac.compare_digest(
            expected, given
        ):
            return self.answer(401, {"error": "bad signature"})
        events = json.loads(raw).get("events", [])
        with open(EVENTS, "a") as f:
            for e in events:
                if e["id"] not in SEEN:
                    SEEN.add(e["id"])
                    f.write(json.dumps(e) + "\n")
        self.answer(200, {"accepted": [e["id"] for e in events]})

    def do_GET(self):
        prefix = "/internal/accounts/"
        if not self.path.startswith(prefix):
            return self.answer(404, {"error": "not found"})
        if (
            self.headers.get("X-Lab-Instance-Id") != INSTANCE
            or self.headers.get("Authorization") != f"Bearer {SECRET}"
        ):
            return self.answer(401, {"error": "unauthorized"})
        try:
            can_spend = open(CAN_SPEND).read().strip() != "false"
        except FileNotFoundError:
            can_spend = True
        account = self.path[len(prefix):]
        self.answer(
            200,
            {
                "active": True,
                "account_id": account,
                "credits": {"balance": 100 if can_spend else 0},
                "can_spend": can_spend,
            },
        )


ThreadingHTTPServer(("127.0.0.1", PORT), Lab).serve_forever()
