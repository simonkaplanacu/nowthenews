#!/usr/bin/env python3
"""Enrichment watchdog — kills the coordinator if Groq errors spike.

Designed to run via launchctl on a 60-second interval. Checks log lines
from the last 5 minutes for rate-limit or billing errors. If the count
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
from datetime import datetime, timedelta
from pathlib import Path

LOG_FILE = Path(__file__).parent.parent / "logs" / "newschat.log"
WATCHDOG_LOG = Path(__file__).parent.parent / "logs" / "watchdog.log"
WINDOW_MINUTES = 5
ERROR_THRESHOLD = 30
ERROR_PATTERNS = ["429", "rate_limit", "Rate limit", "Too Many Requests",
                  "RateLimitError"]

logging.basicConfig(
    filename=str(WATCHDOG_LOG),
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("watchdog")


def count_recent_errors() -> int:
    """Count rate-limit/billing errors in log lines from the last WINDOW_MINUTES."""
    if not LOG_FILE.exists():
        return 0
    try:
        result = subprocess.run(
            ["tail", "-500", str(LOG_FILE)],
            capture_output=True, text=True, timeout=5,
        )
        lines = result.stdout.splitlines()
    except Exception as e:
        log.warning("Failed to read log: %s", e)
        return 0

    cutoff = datetime.now() - timedelta(minutes=WINDOW_MINUTES)
    count = 0
    for line in lines:
        # Parse timestamp from log line: "2026-02-26 06:12:08,045 ..."
        try:
            ts_str = line[:23]
            ts = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S,%f")
        except (ValueError, IndexError):
            continue
        if ts < cutoff:
            continue
        for pattern in ERROR_PATTERNS:
            if pattern in line:
                count += 1
                break
    return count


def find_coordinator_pid() -> int | None:
    """Find the PID of the enrichment coordinator process."""
    try:
        result = subprocess.run(
            ["pgrep", "-f", "enrich_coordinator"],
            capture_output=True, text=True, timeout=5,
        )
        pids = [int(p) for p in result.stdout.strip().split("\n") if p.strip()]
        return max(pids) if pids else None
    except Exception:
        return None


def kill_coordinator(pid: int, error_count: int):
    """Kill the coordinator, log the reason, and write a DB alert."""
    log.warning(
        "Killing coordinator (PID %d) — %d errors in last %d minutes",
        pid, error_count, WINDOW_MINUTES,
    )
    # Write alert to ClickHouse
    try:
        import json
        from newschat.db import write_alert
        write_alert(
            "api_limit", "critical",
            f"Watchdog killed coordinator (PID {pid}) — {error_count} API errors in {WINDOW_MINUTES}min",
            json.dumps({"pid": pid, "error_count": error_count, "window_minutes": WINDOW_MINUTES}),
        )
    except Exception as e:
        log.warning("Failed to write alert to DB: %s", e)

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
        return

    error_count = count_recent_errors()
    if error_count >= ERROR_THRESHOLD:
        kill_coordinator(pid, error_count)
    else:
        log.info("OK — %d errors in last %dm (threshold %d), coordinator PID %d",
                 error_count, WINDOW_MINUTES, ERROR_THRESHOLD, pid)


if __name__ == "__main__":
    main()
