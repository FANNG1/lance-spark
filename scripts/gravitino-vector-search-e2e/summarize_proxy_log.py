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

"""Summarize driver/executor describe-table clients recorded by the REST proxy."""

import argparse
import json
from urllib.parse import urlsplit


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--log", required=True)
    parser.add_argument("--phase", required=True)
    parser.add_argument("--table-suffix", required=True)
    parser.add_argument("--expected-executor-clients", type=int, required=True)
    parser.add_argument("--strict", action="store_true")
    args = parser.parse_args()

    matches = []
    with open(args.log, encoding="utf-8") as source:
        for line in source:
            record = json.loads(line)
            path = record.get("path", "")
            if (
                record.get("phase") == args.phase
                and args.table_suffix in path
                and urlsplit(path).path.endswith("/describe")
            ):
                matches.append(record)

    ports = []
    for record in matches:
        port = record["client_port"]
        if port not in ports:
            ports.append(port)

    # Each spark-shell process resolves the table once on the driver. Any additional client
    # connections in this phase come from executor namespace reconstruction.
    executor_clients = max(0, len(ports) - 1)
    result = {
        "phase": args.phase,
        "describe_requests": len(matches),
        "distinct_client_ports": ports,
        "driver_clients": min(1, len(ports)),
        "executor_clients": executor_clients,
        "expected_executor_clients": args.expected_executor_clients,
    }
    print("CREDENTIALS|" + json.dumps(result, sort_keys=True, separators=(",", ":")))

    if args.strict and executor_clients != args.expected_executor_clients:
        raise SystemExit(
            f"phase {args.phase}: expected {args.expected_executor_clients} executor clients, "
            f"observed {executor_clients}; ports={ports}"
        )


if __name__ == "__main__":
    main()
