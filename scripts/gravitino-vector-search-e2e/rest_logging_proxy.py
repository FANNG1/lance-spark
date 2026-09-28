#!/usr/bin/env python3
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Small reverse proxy that records Lance REST client source ports as JSONL."""

import argparse
import http.client
import json
import ssl
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlsplit


HOP_BY_HOP = {
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailers",
    "transfer-encoding",
    "upgrade",
}


class RequestLog:
    def __init__(self, path: str):
        self.path = path
        self.lock = threading.Lock()

    def append(self, record: dict) -> None:
        line = json.dumps(record, sort_keys=True, separators=(",", ":"))
        with self.lock, open(self.path, "a", encoding="utf-8") as output:
            output.write(line + "\n")


def handler_factory(upstream: str, request_log: RequestLog):
    parsed = urlsplit(upstream)
    if parsed.scheme not in ("http", "https") or not parsed.hostname:
        raise ValueError(f"unsupported upstream URI: {upstream}")
    upstream_path = parsed.path.rstrip("/")
    connection_type = (
        http.client.HTTPSConnection if parsed.scheme == "https" else http.client.HTTPConnection
    )
    upstream_port = parsed.port or (443 if parsed.scheme == "https" else 80)

    class ProxyHandler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, _format: str, *_args) -> None:
            return

        def do_GET(self) -> None:
            self._forward()

        def do_POST(self) -> None:
            self._forward()

        def do_PUT(self) -> None:
            self._forward()

        def do_PATCH(self) -> None:
            self._forward()

        def do_DELETE(self) -> None:
            self._forward()

        def do_HEAD(self) -> None:
            self._forward()

        def _forward(self) -> None:
            if self.path == "/__e2e_health":
                body = b"ok\n"
                self.send_response(200)
                self.send_header("Content-Type", "text/plain")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return

            content_length = int(self.headers.get("Content-Length", "0"))
            body = self.rfile.read(content_length) if content_length else None
            headers = {
                key: value
                for key, value in self.headers.items()
                if key.lower() not in HOP_BY_HOP and key.lower() != "host"
            }
            headers["Host"] = parsed.netloc
            target = self.path
            if upstream_path and not target.startswith(upstream_path + "/"):
                target = upstream_path + (target if target.startswith("/") else "/" + target)

            started = time.time()
            status = 502
            try:
                kwargs = {"timeout": 60}
                if parsed.scheme == "https":
                    kwargs["context"] = ssl.create_default_context()
                conn = connection_type(parsed.hostname, upstream_port, **kwargs)
                conn.request(self.command, target, body=body, headers=headers)
                response = conn.getresponse()
                response_body = response.read()
                status = response.status
                self.send_response(response.status, response.reason)
                for key, value in response.getheaders():
                    if key.lower() not in HOP_BY_HOP and key.lower() != "content-length":
                        self.send_header(key, value)
                self.send_header("Content-Length", str(len(response_body)))
                self.end_headers()
                if self.command != "HEAD":
                    self.wfile.write(response_body)
                conn.close()
            except Exception as error:  # noqa: BLE001 - proxy must turn transport errors into 502
                response_body = str(error).encode("utf-8", errors="replace")
                self.send_response(502)
                self.send_header("Content-Type", "text/plain")
                self.send_header("Content-Length", str(len(response_body)))
                self.end_headers()
                self.wfile.write(response_body)
            finally:
                request_log.append(
                    {
                        "timestamp": time.time(),
                        "elapsed_ms": round((time.time() - started) * 1000, 3),
                        "client_host": self.client_address[0],
                        "client_port": self.client_address[1],
                        "method": self.command,
                        "path": target,
                        "phase": self.headers.get("x-e2e-phase", ""),
                        "status": status,
                    }
                )

    return ProxyHandler


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--listen-host", default="127.0.0.1")
    parser.add_argument("--listen-port", type=int, required=True)
    parser.add_argument("--upstream", required=True)
    parser.add_argument("--log", required=True)
    args = parser.parse_args()

    request_log = RequestLog(args.log)
    server = ThreadingHTTPServer(
        (args.listen_host, args.listen_port),
        handler_factory(args.upstream, request_log),
    )
    server.daemon_threads = True
    server.serve_forever()


if __name__ == "__main__":
    main()
