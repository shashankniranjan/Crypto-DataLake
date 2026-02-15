from __future__ import annotations

import time
from datetime import UTC, datetime, timedelta
from pathlib import Path

import typer
from rich.console import Console

from binance_minute_lake.core.config import Settings
from binance_minute_lake.core.logging import configure_logging
from binance_minute_lake.core.time_utils import floor_to_minute, utc_now
from binance_minute_lake.pipeline.orchestrator import MinuteIngestionPipeline
from binance_minute_lake.sources.metrics_inspector import MetricsZipInspector
from binance_minute_lake.sources.vision import VisionClient
from binance_minute_lake.state.store import SQLiteStateStore

app = typer.Typer(help="Binance minute lake ingestion CLI")
console = Console()


def _parse_utc_datetime(value: str) -> datetime:
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _parse_backfill_loader_target(value: str) -> int | None:
    normalized = value.strip().lower()
    if normalized in {"all", "last-5-years", "last_5_years", "last5"}:
        return None
    try:
        parsed_year = int(normalized)
    except ValueError as exc:
        raise typer.BadParameter("Enter a year like 2024 or 'all' for the last 5 years.") from exc
    if parsed_year < 1970:
        raise typer.BadParameter("year must be >= 1970")
    return parsed_year


def _compute_backfill_loader_window(
    *,
    year: int | None,
    now_utc: datetime,
    safety_lag_minutes: int,
    lookback_years: int = 5,
) -> tuple[datetime, datetime]:
    if now_utc.tzinfo is None:
        now_utc = now_utc.replace(tzinfo=UTC)
    now_utc = now_utc.astimezone(UTC)
    end_utc = floor_to_minute(now_utc - timedelta(minutes=safety_lag_minutes))

    if year is None:
        start_utc = floor_to_minute(end_utc - timedelta(days=365 * lookback_years))
        return start_utc, end_utc

    if year > end_utc.year:
        raise typer.BadParameter(f"year={year} is in the future")

    year_start = datetime(year, 1, 1, 0, 0, tzinfo=UTC)
    year_end = datetime(year, 12, 31, 23, 59, tzinfo=UTC)
    clamped_end = floor_to_minute(min(year_end, end_utc))
    if clamped_end < year_start:
        raise typer.BadParameter(f"year={year} has no ingestible minutes yet")

    return year_start, clamped_end


def _last_completed_utc_day_end(now_utc: datetime) -> datetime:
    current_day_start = floor_to_minute(now_utc.astimezone(UTC)).replace(hour=0, minute=0)
    return current_day_start - timedelta(minutes=1)


def _duckdb_parquet_pattern(root_dir: Path, symbol: str) -> str:
    root_resolved = root_dir.expanduser().resolve()
    return str(
        root_resolved
        / "futures"
        / "um"
        / "minute"
        / f"symbol={symbol.upper()}"
        / "year=*"
        / "month=*"
        / "day=*"
        / "hour=*"
        / "part.parquet"
    )


def _intellij_queries_sql(symbol: str) -> str:
    symbol_upper = symbol.upper()
    return (
        "-- IntelliJ / DataGrip query starter file for minute.duckdb\n"
        "SELECT COUNT(*) AS rows_total FROM minute;\n\n"
        "SELECT *\n"
        f"FROM minute\nWHERE symbol = '{symbol_upper}'\n"
        "ORDER BY timestamp DESC\nLIMIT 200;\n\n"
        "SELECT date_trunc('day', timestamp) AS day_utc, COUNT(*) AS rows_per_day\n"
        f"FROM minute\nWHERE symbol = '{symbol_upper}'\n"
        "GROUP BY 1\n"
        "ORDER BY 1 DESC\nLIMIT 30;\n"
    )


def _materialize_duckdb_view(
    *,
    db_path: Path,
    parquet_root: Path,
    symbol: str,
    write_queries: bool,
) -> tuple[Path, Path | None]:
    try:
        import duckdb  # type: ignore
    except ImportError as exc:
        console.print("[red]duckdb not installed. Install with `pip install duckdb`.[/red]")
        raise typer.Exit(code=1) from exc

    db_path_resolved = db_path.expanduser().resolve()
    db_path_resolved.parent.mkdir(parents=True, exist_ok=True)

    pattern = _duckdb_parquet_pattern(parquet_root, symbol)
    pattern_sql = pattern.replace("'", "''")

    console.print(f"Building DuckDB at {db_path_resolved} from {pattern}")

    con = duckdb.connect(str(db_path_resolved))
    con.execute(
        f"""
        CREATE OR REPLACE VIEW minute AS
        SELECT
            *,
            regexp_extract(filename, 'symbol=([^/]+)', 1) AS symbol,
            regexp_extract(filename, 'year=([0-9]{{4}})', 1) AS year,
            regexp_extract(filename, 'month=([0-9]{{2}})', 1) AS month,
            regexp_extract(filename, 'day=([0-9]{{2}})', 1) AS day,
            regexp_extract(filename, 'hour=([0-9]{{2}})', 1) AS hour
        FROM read_parquet('{pattern_sql}', hive_partitioning=true, filename=true);
        """
    )
    con.close()

    query_file: Path | None = None
    if write_queries:
        query_file = db_path_resolved.with_name(f"{db_path_resolved.stem}.queries.sql")
        query_file.write_text(_intellij_queries_sql(symbol), encoding="utf-8")

    return db_path_resolved, query_file


@app.command("init-state")
def init_state() -> None:
    settings = Settings()
    configure_logging(settings.log_level)
    store = SQLiteStateStore(settings.state_db)
    store.initialize()
    console.print(f"State initialized at [bold]{settings.state_db}[/bold]")


@app.command("show-watermark")
def show_watermark(symbol: str | None = None) -> None:
    settings = Settings()
    configure_logging(settings.log_level)
    store = SQLiteStateStore(settings.state_db)
    store.initialize()
    symbol_value = (symbol or settings.symbol).upper()
    watermark = store.get_watermark(symbol_value)
    if watermark is None:
        console.print(f"No watermark found for {symbol_value}")
        return
    console.print(f"Watermark[{symbol_value}] = [bold]{watermark.isoformat()}[/bold]")


@app.command("run-once")
def run_once(
    at: str | None = typer.Option(default=None, help="Optional UTC ISO datetime"),
    max_hours: int | None = typer.Option(
        default=None,
        min=1,
        max=24,
        help="Optional cap on hours processed in this invocation",
    ),
) -> None:
    settings = Settings()
    configure_logging(settings.log_level)

    run_at: datetime | None = None
    if at is not None:
        run_at = datetime.fromisoformat(at)
        if run_at.tzinfo is None:
            run_at = run_at.replace(tzinfo=UTC)

    pipeline = MinuteIngestionPipeline(settings=settings)
    try:
        summary = pipeline.run_once(now=run_at, max_hours=max_hours)
    finally:
        pipeline.close()

    console.print(
        "Run complete: "
        f"symbol={summary.symbol}, "
        f"partitions={summary.partitions_committed}, "
        f"watermark_before={summary.watermark_before.isoformat()}, "
        f"watermark_after={summary.watermark_after.isoformat()}, "
        f"target={summary.target_horizon.isoformat()}"
    )


@app.command("run-daemon")
def run_daemon(
    poll_seconds: int = typer.Option(default=60, min=5, help="Polling interval in seconds"),
) -> None:
    settings = Settings()
    configure_logging(settings.log_level)

    pipeline = MinuteIngestionPipeline(settings=settings)
    try:
        pipeline.run_daemon(poll_seconds=poll_seconds)
    finally:
        pipeline.close()


@app.command("run-forever")
def run_forever(
    poll_seconds: int = typer.Option(default=60, min=5, help="Aligns to minute grid; sleeps between polls"),
) -> None:
    """
    Minute daemon that polls continuously and aligns sleep to the next minute boundary.
    """
    settings = Settings()
    configure_logging(settings.log_level)

    pipeline = MinuteIngestionPipeline(settings=settings)
    try:
        while True:
            try:
                summary = pipeline.run_once()
                console.print(
                    "Tick: "
                    f"partitions={summary.partitions_committed}, "
                    f"wm={summary.watermark_after.isoformat()}, "
                    f"target={summary.target_horizon.isoformat()}"
                )
            except Exception as exc:  # pylint: disable=broad-except
                console.print(f"[red]Tick failed:[/red] {exc}")
            now = utc_now()
            # align to the next poll boundary to avoid drift
            remainder = (now.timestamp()) % poll_seconds
            sleep_seconds = poll_seconds - remainder
            if sleep_seconds < 0.05:
                sleep_seconds = poll_seconds
            time.sleep(sleep_seconds)
    finally:
        pipeline.close()


@app.command("inspect-metrics-columns")
def inspect_metrics_columns(
    trade_date: str = typer.Option(help="Date in YYYY-MM-DD format"),
    symbol: str | None = typer.Option(default=None),
) -> None:
    settings = Settings()
    configure_logging(settings.log_level)
    symbol_value = (symbol or settings.symbol).upper()
    parsed_date = datetime.strptime(trade_date, "%Y-%m-%d").date()

    vision = VisionClient(base_url=settings.vision_base_url)
    try:
        url = vision.build_daily_zip_url("metrics", symbol_value, parsed_date)
        destination = settings.root_dir / ".cache" / f"{symbol_value}-metrics-{trade_date}.zip"
        vision.download_zip(url=url, destination=destination)
    finally:
        vision.close()

    columns = MetricsZipInspector.list_columns(destination)
    console.print(f"Metrics columns ({len(columns)}):")
    for column in columns:
        console.print(f" - {column}")


@app.command("materialize-duckdb")
def materialize_duckdb(
    db_path: Path = typer.Option(Path("data/minute.duckdb"), help="Output DuckDB file"),
    symbol: str = typer.Option("BTCUSDT", help="Symbol to index"),
    parquet_root: Path | None = typer.Option(
        default=None,
        help="Root data directory containing futures/um/minute (default: BML_ROOT_DIR)",
    ),
    write_queries: bool = typer.Option(
        True,
        "--write-queries/--no-write-queries",
        help="Write a SQL starter file beside the DuckDB file for IntelliJ queries.",
    ),
) -> None:
    """
    Create/update a DuckDB database with a view over all parquet partitions for IDE browsing (DataGrip/IntelliJ).
    """
    settings = Settings()
    root = parquet_root or settings.root_dir
    out_db, query_file = _materialize_duckdb_view(
        db_path=db_path,
        parquet_root=root,
        symbol=symbol,
        write_queries=write_queries,
    )
    console.print(f"[green]DuckDB view `minute` ready at {out_db}.[/green]")
    if query_file is not None:
        console.print(f"[green]IntelliJ SQL starter created at {query_file}.[/green]")


@app.command("prepare-intellij")
def prepare_intellij(
    db_path: Path = typer.Option(Path("data/minute.duckdb"), help="Output DuckDB file"),
    symbol: str = typer.Option("BTCUSDT", help="Symbol to index"),
    parquet_root: Path | None = typer.Option(
        default=None,
        help="Root data directory containing futures/um/minute (default: BML_ROOT_DIR)",
    ),
) -> None:
    """
    IntelliJ helper: materialize a DuckDB view over parquet partitions with absolute paths.
    """
    settings = Settings()
    root = parquet_root or settings.root_dir
    out_db, query_file = _materialize_duckdb_view(
        db_path=db_path,
        parquet_root=root,
        symbol=symbol,
        write_queries=True,
    )
    console.print("[green]IntelliJ dataset prepared.[/green]")
    console.print(f"DuckDB file: {out_db}")
    if query_file is not None:
        console.print(f"Query file: {query_file}")


@app.command("backfill-range")
def backfill_range(
    start: str = typer.Option(help="Start datetime in ISO format (UTC if no timezone)"),
    end: str | None = typer.Option(
        default=None,
        help="End datetime in ISO format (default: now-safety-lag)",
    ),
    sleep_seconds: float = typer.Option(
        default=0.0,
        min=0.0,
        max=10.0,
        help="Optional sleep between repaired hours to reduce API pressure",
    ),
    max_missing_hours: int | None = typer.Option(
        default=None,
        min=1,
        help="Optional cap for number of missing/invalid hours repaired in this invocation",
    ),
) -> None:
    settings = Settings()
    configure_logging(settings.log_level)

    start_utc = floor_to_minute(_parse_utc_datetime(start))
    if end is None:
        end_utc = floor_to_minute(utc_now() - timedelta(minutes=settings.safety_lag_minutes))
    else:
        end_utc = floor_to_minute(_parse_utc_datetime(end))

    if end_utc < start_utc:
        raise typer.BadParameter("end must be >= start")

    pipeline = MinuteIngestionPipeline(settings=settings)
    try:
        summary = pipeline.run_consistency_backfill(
            start=start_utc,
            end=end_utc,
            now_for_band=utc_now(),
            sleep_seconds=sleep_seconds,
            max_missing_hours=max_missing_hours,
        )
    finally:
        pipeline.close()

    console.print(
        "Backfill consistency: "
        f"hours_scanned={summary.hours_scanned}, "
        f"issues_found={summary.issues_found}, "
        f"issues_targeted={summary.issues_targeted}, "
        f"hours_repaired={summary.hours_repaired}, "
        f"hours_failed={summary.hours_failed}, "
        f"issues_remaining={summary.issues_remaining}"
    )

    if max_missing_hours is None and summary.issues_remaining > 0:
        raise typer.Exit(code=1)


@app.command("backfill-years")
def backfill_years(
    years: int = typer.Option(default=5, min=1, max=10),
    sleep_seconds: float = typer.Option(default=0.0, min=0.0, max=10.0),
    max_missing_hours: int | None = typer.Option(default=None, min=1),
) -> None:
    settings = Settings()
    configure_logging(settings.log_level)

    end_utc = floor_to_minute(utc_now() - timedelta(minutes=settings.safety_lag_minutes))
    start_utc = floor_to_minute(end_utc - timedelta(days=365 * years))

    pipeline = MinuteIngestionPipeline(settings=settings)
    try:
        summary = pipeline.run_consistency_backfill(
            start=start_utc,
            end=end_utc,
            now_for_band=utc_now(),
            sleep_seconds=sleep_seconds,
            max_missing_hours=max_missing_hours,
        )
    finally:
        pipeline.close()

    console.print(
        "Backfill consistency: "
        f"hours_scanned={summary.hours_scanned}, "
        f"issues_found={summary.issues_found}, "
        f"issues_targeted={summary.issues_targeted}, "
        f"hours_repaired={summary.hours_repaired}, "
        f"hours_failed={summary.hours_failed}, "
        f"issues_remaining={summary.issues_remaining}"
    )

    if max_missing_hours is None and summary.issues_remaining > 0:
        raise typer.Exit(code=1)


@app.command("backfill-loader")
def backfill_loader(
    year: int | None = typer.Option(
        default=None,
        min=1970,
        help="Specific UTC year to validate and repair (example: 2024).",
    ),
    last_5_years: bool = typer.Option(
        False,
        "--last-5-years",
        help="Ignore --year and repair missing data over the trailing 5 years.",
    ),
    sleep_seconds: float = typer.Option(
        default=0.0,
        min=0.0,
        max=10.0,
        help="Optional sleep between repaired hours to reduce API pressure",
    ),
    max_missing_hours: int | None = typer.Option(
        default=None,
        min=1,
        help="Optional cap for number of missing/invalid hours repaired in this invocation",
    ),
    vision_only: bool = typer.Option(
        True,
        "--vision-only/--allow-rest-fallback",
        help=(
            "Use Binance Vision daily files only (recommended during REST bans). "
            "Use --allow-rest-fallback to call REST when Vision files are unavailable."
        ),
    ),
) -> None:
    settings = Settings()
    configure_logging(settings.log_level)

    if year is not None and last_5_years:
        raise typer.BadParameter("Choose either --year or --last-5-years, not both.")

    selected_year = year
    if selected_year is None and not last_5_years:
        target_value = typer.prompt(
            "Enter year (YYYY) to repair, or type 'all' for the last 5 years",
            default="all",
        )
        selected_year = _parse_backfill_loader_target(target_value)
    if last_5_years:
        selected_year = None

    now_utc = utc_now()
    start_utc, end_utc = _compute_backfill_loader_window(
        year=selected_year,
        now_utc=now_utc,
        safety_lag_minutes=settings.safety_lag_minutes,
        lookback_years=5,
    )
    if vision_only:
        end_utc = min(end_utc, _last_completed_utc_day_end(now_utc))
        if end_utc < start_utc:
            raise typer.BadParameter(
                "No completed UTC day is available for Vision in this range yet. "
                "Try an older year or use --allow-rest-fallback."
            )

    scope = f"year={selected_year}" if selected_year is not None else "last_5_years"
    console.print(
        "Backfill loader scope: "
        f"{scope}, start={start_utc.isoformat()}, end={end_utc.isoformat()}, vision_only={vision_only}"
    )

    pipeline = MinuteIngestionPipeline(settings=settings)
    try:
        summary = pipeline.run_consistency_backfill(
            start=start_utc,
            end=end_utc,
            now_for_band=now_utc,
            sleep_seconds=sleep_seconds,
            max_missing_hours=max_missing_hours,
            force_cold_band=vision_only,
            include_rest_enrichment=not vision_only,
            allow_rest_fallback=not vision_only,
        )
    finally:
        pipeline.close()

    console.print(
        "Backfill loader: "
        f"hours_scanned={summary.hours_scanned}, "
        f"issues_found={summary.issues_found}, "
        f"issues_targeted={summary.issues_targeted}, "
        f"hours_repaired={summary.hours_repaired}, "
        f"hours_failed={summary.hours_failed}, "
        f"issues_remaining={summary.issues_remaining}"
    )

    if max_missing_hours is None and summary.issues_remaining > 0:
        raise typer.Exit(code=1)


if __name__ == "__main__":
    app()
