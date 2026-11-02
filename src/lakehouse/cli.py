"""Command-line entry point: `lakehouse ingest <source>`."""

from __future__ import annotations

import argparse
import logging
import os
import time

from lakehouse.config import load_sources
from lakehouse.ingestion.socrata import SocrataClient
from lakehouse.logging_setup import configure_logging, run_context
from lakehouse.storage.bronze import DEFAULT_DB, BronzeStore

log = logging.getLogger("lakehouse.cli")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="lakehouse")
    parser.add_argument(
        "--log-json", action="store_true", help="Emit logs as JSON lines (one object per line)"
    )
    parser.add_argument("--log-level", default="INFO", help="Logging level (default: INFO)")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("sources", help="List configured sources")

    ingest = sub.add_parser("ingest", help="Ingest a source into the bronze layer")
    ingest.add_argument("source")
    ingest.add_argument("--max-records", type=int, default=5000)
    ingest.add_argument("--full", action="store_true", help="Ignore the stored watermark")
    ingest.add_argument("--db", default=str(DEFAULT_DB))
    ingest.add_argument("--run-id", default=None, help="Override the generated run ID")

    args = parser.parse_args(argv)
    configure_logging(level=args.log_level.upper(), json_output=args.log_json)
    sources = load_sources()

    if args.command == "sources":
        for name, src in sources.items():
            print(f"{name:<24} {src.endpoint}  — {src.description}")
        return 0

    if args.source not in sources:
        parser.error(f"Unknown source '{args.source}'. Try: {', '.join(sources)}")
    src = sources[args.source]

    with run_context(args.run_id) as run_id:
        t0 = time.monotonic()
        store = BronzeStore(args.db)
        since = None if args.full else store.last_watermark(src.name)
        log.info(
            "Ingestion started for %s",
            src.name,
            extra={"source": src.name, "since": since, "max_records": args.max_records},
        )
        try:
            client = SocrataClient(app_token=os.getenv("SOCRATA_APP_TOKEN"))
            n = store.write(
                src.name,
                client.fetch(src, since=since, max_records=args.max_records),
                primary_key=src.primary_key,
                incremental_column=src.incremental_column,
                run_id=run_id,
            )
        except Exception:
            log.exception("Ingestion failed for %s", src.name, extra={"source": src.name})
            return 1
        finally:
            store.close()
        log.info(
            "Ingestion finished for %s",
            src.name,
            extra={
                "source": src.name,
                "records": n,
                "duration_s": round(time.monotonic() - t0, 3),
            },
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
