#!/usr/bin/env python3
"""Enrichment watchdog — kills the coordinator if Groq 429 errors spike.

Designed to run via launchctl on a 60-second interval. Checks the last
200 lines of the enrichment log for rate-limit errors. If the count
exceeds the threshold, kills the coordinator process and logs the action.

Usage (standalone test):
    python scripts/enrichment_watchdog.py

Managed by launchctl:
    launchctl load ~/Library/LaunchAgents/com.nowthenews.enrichment-watchdog.plist
"""

import logging
import os
import signal
import subprocess
import sys
from pathlib import Path

LOG_FILE = Path(__file__).parent.parent / "logs" / "newschat.log"
WATCHDOG_LOG = Path(__file__).parent.parent / "logs" / "watchdog.log"
TAIL_LINES = 200
ERROR_THRESHOLD = 5  # kill if this many 429s in last TAIL_LINES
ERROR_PATTERNS = ["429", "rate_limit", "Rate limit", "Too Many Requests", "RateLimitError", "400 Bad Request"]

logging.basicConfig(
    filename=str(WATCHDOG_LOG),
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("watchdog")


def count_errors() -> int:
    """Count rate-limit errors in the tail of the enrichment log."""
    if not LOG_FILE.exists():
        return 0
    try:
        result = subprocess.run(
            ["tail", f"-{TAIL_LINES}", str(LOG_FILE)],
            capture_output=True, text=True, timeout=5,
        )
        lines = result.stdout
    except Exception as e:
        log.warning("Failed to read log: %s", e)
        return 0

    count = 0
    for pattern in ERROR_PATTERNS:
        count += lines.count(pattern)
    return count


def find_coordinator_pid() -> int | None:
    """Find the PID of the enrichment coordinator process."""
    try:
        result = subprocess.run(
            ["pgrep", "-f", "enrich_coordinator"],
            capture_output=True, text=True, timeout=5,
        )
        pids = [int(p) for p in result.stdout.strip().split("\n") if p.strip()]
        # Return the actual python process, not the shell wrapper
        return max(pids) if pids else None
    except Exception:
        return None


def kill_coordinator(pid: int, error_count: int):
    """Kill the coordinator and log the reason."""
    log.warning(
        "Killing coordinator (PID %d) — %d rate-limit errors in last %d log lines",
        pid, error_count, TAIL_LINES,
    )
    try:
        os.kill(pid, signal.SIGTERM)
        log.info("Sent SIGTERM to PID %d", pid)
    except ProcessLookupError:
        log.info("PID %d already gone", pid)
    except Exception as e:
        log.error("Failed to kill PID %d: %s", pid, e)


def main():
    pid = find_coordinator_pid()
    if pid is None:
        # No coordinator running — nothing to watch
        return

    error_count = count_errors()
    if error_count >= ERROR_THRESHOLD:
        kill_coordinator(pid, error_count)
    else:
        log.debug("OK — %d errors (threshold %d), coordinator PID %d", error_count, ERROR_THRESHOLD, pid)


if __name__ == "__main__":
    main()
