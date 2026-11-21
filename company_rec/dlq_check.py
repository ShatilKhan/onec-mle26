"""One-shot DLQ depth check. Exit 1 when depth exceeds baseline or grew since last check."""

from __future__ import annotations

import argparse
import base64
import json
import os
import urllib.request
from pathlib import Path

from company_rec.config import Settings

STATE = Path(os.environ.get("DLQ_STATE_FILE", "/tmp/opencode/dlq-depth.txt"))


def depth(settings: Settings) -> int:
    url = f"http://127.0.0.1:15672/api/queues/%2f/{settings.dlq}"
    token = os.environ.get("RABBITMQ_MGMT_AUTH", "guest:guest")
    request = urllib.request.Request(url)
    request.add_header("Authorization", "Basic " + base64.b64encode(token.encode()).decode())
    with urllib.request.urlopen(request, timeout=5) as response:
        return int(json.load(response)["messages"])


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    args = parser.parse_args()
    settings = Settings.from_env()
    current = depth(settings)
    previous = int(STATE.read_text().strip()) if STATE.exists() else current
    STATE.parent.mkdir(parents=True, exist_ok=True)
    STATE.write_text(str(current))
    if current > settings.dlq_baseline or current > previous:
        raise SystemExit(f"DLQ depth {current} (baseline {settings.dlq_baseline}, previous {previous})")
    print(f"DLQ depth {current} OK")


if __name__ == "__main__":
    main()
