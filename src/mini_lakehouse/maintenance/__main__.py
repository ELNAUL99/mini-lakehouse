"""Maintenance entrypoint.

In production this runs as a Kubernetes CronJob (design doc section 9); locally it
can run a single pass (``--once``) or a simple internal scheduler. Kept small: the
scheduling policy lives in the deployment, not the code.
"""

from __future__ import annotations

import argparse
import logging
import time

from .jobs import MaintenanceWorker

log = logging.getLogger("maintenance")


def run_pass(worker: MaintenanceWorker, table: str) -> None:
    worker.compact(table)
    report = worker.health_report(table)
    log.info("%s health: %s", table, report.as_row())


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    parser = argparse.ArgumentParser(prog="ml-maintenance")
    parser.add_argument("--table", default="bronze.events")
    parser.add_argument("--once", action="store_true", help="run one pass and exit")
    parser.add_argument("--interval-s", type=int, default=900, help="loop interval (default 15m)")
    args = parser.parse_args()

    worker = MaintenanceWorker()
    if args.once:
        run_pass(worker, args.table)
        return
    while True:  # pragma: no cover
        run_pass(worker, args.table)
        time.sleep(args.interval_s)


if __name__ == "__main__":  # pragma: no cover
    main()
