#!/usr/bin/env python3
"""A local receiver for Grafana's webhook contact point.

Grafana posts each notification as one JSON body. Writing it to a file is what
lets the E2E say an alert not only fired but was routed: the contact point's
URL is this server, and the file is the evidence.
"""

import http.server
import sys

LOG = sys.argv[1]
PORT = int(sys.argv[2])


class Handler(http.server.BaseHTTPRequestHandler):
    def do_POST(self) -> None:  # noqa: N802 - the stdlib's name
        length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(length)
        with open(LOG, "ab") as log:
            log.write(body)
            log.write(b"\n")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(b"{}")

    def log_message(self, *args: object) -> None:
        pass


http.server.HTTPServer(("127.0.0.1", PORT), Handler).serve_forever()
