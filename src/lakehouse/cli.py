"""Command-line entry point: `lakehouse ingest <source>`."""

from __future__ import annotations

import argparse
import logging
import os
import time

from lakehouse.config import load_sources
from lakehouse.drift import SEVERITIES, compare
from lakehouse.ingestion.socrata import SocrataClient
from lakehouse.logging_setup import configure_logging, run_context
from lakehouse.schema import format_schema
from lakehouse.silver import crimes as silver_crimes
from lakehouse.status import collect_status, format_status, status_json
from lakehouse.storage.bronze import DEFAULT_DB, BronzeStore
from lakehouse.storage.export import DEFAULT_EXPORT_DIR, export_all, export_source

log = logging.getLogger("lakehouse.cli")

SILVER_BUILDERS = {"crimes": silver_crimes.build}


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
    ingest.add_argument(
        "--fail-on-drift",
        choices=SEVERITIES,
        default=None,
        help="Exit with status 2 if this run's schema drift reaches this severity",
    )

    dedupe = sub.add_parser(
        "dedupe", help="Report duplicate versions in bronze and compact to the latest per key"
    )
    dedupe.add_argument("source")
    dedupe.add_argument("--db", default=str(DEFAULT_DB))
    dedupe.add_argument(
        "--dry-run", action="store_true", help="Only report counts; do not remove rows"
    )

    schema = sub.add_parser("schema", help="Show the inferred schema captured for a source")
    schema.add_argument("source")
    schema.add_argument("--db", default=str(DEFAULT_DB))
    schema.add_argument("--run-id", default=None, help="Snapshot of a specific run")
    schema.add_argument("--history", action="store_true", help="List all snapshots")
    schema.add_argument("--json", action="store_true", help="Print the snapshot as JSON")

    drift = sub.add_parser("drift", help="Compare schema snapshots between runs")
    drift.add_argument("source")
    drift.add_argument("--db", default=str(DEFAULT_DB))
    drift.add_argument("--from", dest="from_run", default=None, help="Baseline run ID")
    drift.add_argument("--to", dest="to_run", default=None, help="Compared run ID")
    drift.add_argument("--json", action="store_true", help="Print the report as JSON")
    drift.add_argument("--history", action="store_true", help="List stored drift events")
    drift.add_argument(
        "--fail-on",
        choices=SEVERITIES,
        default=None,
        help="Exit with status 2 if drift reaches this severity",
    )

    export = sub.add_parser(
        "export", help="Export bronze tables to Parquet, partitioned by ingest date"
    )
    export.add_argument("source", nargs="?", help="Source to export (default: all)")
    export.add_argument("--db", default=str(DEFAULT_DB))
    export.add_argument("--out", default=str(DEFAULT_EXPORT_DIR), help="Output directory")
    export.add_argument(
        "--latest", action="store_true", help="Export the latest-per-key view instead"
    )

    status = sub.add_parser("status", help="Ingestion metrics per source")
    status.add_argument("--db", default=str(DEFAULT_DB))
    status.add_argument("--json", action="store_true", help="Print metrics as JSON")
    status.add_argument(
        "--stale-hours",
        type=float,
        default=36.0,
        help="Flag sources whose last run is older than this (default: 36)",
    )

    silver = sub.add_parser("silver", help="Build a silver table from bronze")
    silver.add_argument("table", choices=sorted(SILVER_BUILDERS))
    silver.add_argument("--db", default=str(DEFAULT_DB))

    args = parser.parse_args(argv)
    configure_logging(level=args.log_level.upper(), json_output=args.log_json)
    sources = load_sources()

    if args.command == "sources":
        for name, src in sources.items():
            print(f"{name:<24} {src.endpoint}  — {src.description}")
        return 0

    if args.command == "dedupe":
        return _dedupe(args.source, args.db, args.dry_run)

    if args.command == "schema":
        return _schema(args)

    if args.command == "drift":
        return _drift(args)

    if args.command == "export":
        return _export(args)

    if args.command == "silver":
        store = BronzeStore(args.db)
        try:
            result = SILVER_BUILDERS[args.table](store)
        except ValueError as exc:
            log.error("%s", exc, extra={"table": args.table})
            return 1
        finally:
            store.close()
        print(f"{result.table}: {result.rows} rows from {result.source}")
        for col, n in sorted(result.cast_failures.items()):
            print(f"  cast failures: {col}={n}")
        return 0

    if args.command == "status":
        store = BronzeStore(args.db)
        try:
            report = collect_status(store, sources, stale_hours=args.stale_hours)
        finally:
            store.close()
        print(status_json(report) if args.json else format_status(report))
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
                request_stats=getattr(client, "stats", None),
            )
            drift_report = store.last_drift
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
        if args.fail_on_drift and drift_report and drift_report.at_least(args.fail_on_drift):
            log.error(
                "Schema drift for %s reached '%s'; failing as requested",
                src.name,
                drift_report.severity,
                extra={"source": src.name, "drift_severity": drift_report.severity},
            )
            return 2
    return 0


def _dedupe(source: str, db: str, dry_run: bool) -> int:
    store = BronzeStore(db)
    try:
        exists = store.con.execute(
            "SELECT count(*) FROM information_schema.tables "
            "WHERE table_schema = 'bronze' AND table_name = ?",
            [source],
        ).fetchone()[0]
        if not exists:
            log.error("No bronze table for source '%s'", source, extra={"source": source})
            return 1
        stats = store.dedupe_stats(source)
        print(
            f"bronze.{source}: {stats.rows} rows, {stats.keys} keys, "
            f"{stats.duplicates} superseded"
        )
        if not dry_run and stats.duplicates:
            removed = store.compact(source)
            print(f"Removed {removed} superseded rows")
    finally:
        store.close()
    return 0


def _schema(args: argparse.Namespace) -> int:
    store = BronzeStore(args.db)
    try:
        if args.history:
            history = store.schema_history(args.source)
            for snap in history:
                print(
                    f"{snap.captured_at}  run={snap.run_id or '-'}  records={snap.record_count}"
                    f"  fields={len(snap.fields)}  fingerprint={snap.fingerprint}"
                )
            snap = history[-1] if history else None
        else:
            snap = store.schema_snapshot(args.source, args.run_id)
            if snap is not None:
                print(snap.to_json() if args.json else format_schema(snap))
    finally:
        store.close()
    if snap is None:
        log.error(
            "No schema snapshot for '%s'%s",
            args.source,
            f" run {args.run_id}" if args.run_id else "",
            extra={"source": args.source},
        )
        return 1
    return 0


def _drift(args: argparse.Namespace) -> int:
    store = BronzeStore(args.db)
    try:
        if args.history:
            events = store.drift_history(args.source)
            for e in events:
                types = f" {e['old_type'] or '-'} -> {e['new_type'] or '-'}"
                print(
                    f"{e['detected_at']}  {e['old_run_id']} -> {e['new_run_id']}  "
                    f"[{e['severity']}] {e['kind']} {e['field']}{types}"
                )
            if not events:
                print(f"no drift recorded for {args.source}")
            return 0
        history = store.schema_history(args.source)
    finally:
        store.close()

    by_run = {s.run_id: s for s in history}
    for run in (args.from_run, args.to_run):
        if run is not None and run not in by_run:
            log.error("No schema snapshot for run '%s'", run, extra={"source": args.source})
            return 1
    new = by_run[args.to_run] if args.to_run else (history[-1] if history else None)
    if args.from_run:
        old = by_run[args.from_run]
    else:
        earlier = history[: history.index(new)] if new is not None else []
        old = earlier[-1] if earlier else None
    if old is None or new is None:
        log.error(
            "Need two schema snapshots of '%s' to compare",
            args.source,
            extra={"source": args.source},
        )
        return 1

    report = compare(old, new)
    print(report.to_json() if args.json else report.format())
    if args.fail_on and report.at_least(args.fail_on):
        return 2
    return 0


def _export(args: argparse.Namespace) -> int:
    store = BronzeStore(args.db)
    try:
        if args.source:
            results = [export_source(store, args.source, args.out, args.latest)]
        else:
            results = export_all(store, args.out, args.latest)
    except ValueError as exc:
        log.error("%s", exc, extra={"source": args.source})
        return 1
    finally:
        store.close()
    for r in results:
        print(f"{r.source}: {r.rows} rows in {len(r.partitions)} partitions -> {r.path}")
    if not results:
        print("no bronze tables to export")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
