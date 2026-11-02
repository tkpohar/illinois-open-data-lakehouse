"""Command-line entry point: `lakehouse ingest <source>`."""

from __future__ import annotations

import argparse
import logging
import os

from lakehouse.config import load_sources
from lakehouse.ingestion.socrata import SocrataClient
from lakehouse.storage.bronze import DEFAULT_DB, BronzeStore


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="lakehouse")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("sources", help="List configured sources")

    ingest = sub.add_parser("ingest", help="Ingest a source into the bronze layer")
    ingest.add_argument("source")
    ingest.add_argument("--max-records", type=int, default=5000)
    ingest.add_argument("--full", action="store_true", help="Ignore the stored watermark")
    ingest.add_argument("--db", default=str(DEFAULT_DB))

    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    sources = load_sources()

    if args.command == "sources":
        for name, src in sources.items():
            print(f"{name:<24} {src.endpoint}  — {src.description}")
        return 0

    if args.source not in sources:
        parser.error(f"Unknown source '{args.source}'. Try: {', '.join(sources)}")
    src = sources[args.source]
    store = BronzeStore(args.db)
    since = None if args.full else store.last_watermark(src.name)
    client = SocrataClient(app_token=os.getenv("SOCRATA_APP_TOKEN"))
    n = store.write(
        src.name,
        client.fetch(src, since=since, max_records=args.max_records),
        primary_key=src.primary_key,
        incremental_column=src.incremental_column,
    )
    logging.info("Ingested %d records into bronze.%s (since=%s)", n, src.name, since)
    store.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
