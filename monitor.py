from __future__ import annotations

import csv
import argparse
import json
import math
import sqlite3
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, TextIO


class ConfigError(ValueError):
    pass


class EngineConnectionError(RuntimeError):
    pass


@dataclass(frozen=True)
class MonitorConfig:
    containers: tuple[str, ...]
    poll_interval_seconds: float
    database_path: Path


@dataclass(frozen=True)
class MetricsSample:
    container: str
    timestamp: float
    cpu_total_ns: int | None = None
    system_cpu_total: int | None = None
    online_cpus: int | None = None
    memory_usage_bytes: int | None = None
    memory_raw_bytes: int | None = None
    memory_limit_bytes: int | None = None
    memory_peak_bytes: int | None = None
    network_rx_bytes: int | None = None
    network_tx_bytes: int | None = None
    block_read_bytes: int | None = None
    block_write_bytes: int | None = None


RAW_FIELDS = (
    "cpu_total_ns",
    "system_cpu_total",
    "online_cpus",
    "memory_usage_bytes",
    "memory_raw_bytes",
    "memory_limit_bytes",
    "memory_peak_bytes",
    "network_rx_bytes",
    "network_tx_bytes",
    "block_read_bytes",
    "block_write_bytes",
)
DERIVED_FIELDS = (
    "cpu_percent",
    "network_rx_bytes_per_second",
    "network_tx_bytes_per_second",
    "block_read_bytes_per_second",
    "block_write_bytes_per_second",
)


def default_config_path() -> Path:
    return Path(__file__).resolve().with_name("containers.json")


def load_config(path: str | Path, require_containers: bool = True) -> MonitorConfig:
    config_path = Path(path).expanduser().resolve()
    try:
        with config_path.open(encoding="utf-8") as config_file:
            data = json.load(config_file)
    except FileNotFoundError as error:
        raise ConfigError(f"Configuration file not found: {config_path}") from error
    except json.JSONDecodeError as error:
        raise ConfigError(f"Invalid JSON in {config_path}: {error.msg}") from error
    except OSError as error:
        raise ConfigError(f"Cannot read configuration file {config_path}: {error}") from error

    if not isinstance(data, dict):
        raise ConfigError("Configuration must be a JSON object.")

    containers = data.get("containers")
    if not isinstance(containers, list) or (require_containers and not containers):
        raise ConfigError("'containers' must be a list of container names or IDs and cannot be empty for monitoring.")
    if any(not isinstance(name, str) or not name.strip() for name in containers):
        raise ConfigError("Every item in 'containers' must be a non-empty string.")
    normalized_containers = tuple(name.strip() for name in containers)
    if len(set(normalized_containers)) != len(normalized_containers):
        raise ConfigError("'containers' must not contain duplicate names or IDs.")

    interval = data.get("poll_interval_seconds", 2)
    if isinstance(interval, bool) or not isinstance(interval, (int, float)):
        raise ConfigError("'poll_interval_seconds' must be a positive number.")
    interval = float(interval)
    if not math.isfinite(interval) or interval <= 0:
        raise ConfigError("'poll_interval_seconds' must be a positive finite number.")

    database = data.get("database", "metrics.sqlite3")
    if not isinstance(database, str) or not database.strip():
        raise ConfigError("'database' must be a non-empty file path.")
    database_path = Path(database).expanduser()
    if not database_path.is_absolute():
        database_path = config_path.parent / database_path

    return MonitorConfig(normalized_containers, interval, database_path.resolve())


def _integer(value: Any) -> int | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    return int(value)


def _sum_counter_values(entries: Any, counter_name: str) -> int | None:
    if not isinstance(entries, dict) or not entries:
        return None
    values = [_integer(interface.get(counter_name)) for interface in entries.values() if isinstance(interface, dict)]
    available = [value for value in values if value is not None]
    return sum(available) if available else None


def _block_counters(stats: Mapping[str, Any]) -> tuple[int | None, int | None]:
    block_stats = stats.get("blkio_stats")
    entries = block_stats.get("io_service_bytes_recursive") if isinstance(block_stats, dict) else None
    if not isinstance(entries, list) or not entries:
        return None, None

    totals = {"read": 0, "write": 0}
    found = {"read": False, "write": False}
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        operation = entry.get("op")
        value = _integer(entry.get("value"))
        if isinstance(operation, str) and operation.casefold() in totals and value is not None:
            key = operation.casefold()
            totals[key] += value
            found[key] = True
    return (
        totals["read"] if found["read"] else None,
        totals["write"] if found["write"] else None,
    )


def parse_stats(
    container: str,
    stats: Mapping[str, Any],
    timestamp: float | None = None,
) -> MetricsSample:
    cpu_stats = stats.get("cpu_stats")
    cpu_stats = cpu_stats if isinstance(cpu_stats, dict) else {}
    cpu_usage = cpu_stats.get("cpu_usage")
    cpu_usage = cpu_usage if isinstance(cpu_usage, dict) else {}
    per_cpu = cpu_usage.get("percpu_usage")
    online_cpus = _integer(cpu_stats.get("online_cpus"))
    if online_cpus is None and isinstance(per_cpu, list) and per_cpu:
        online_cpus = len(per_cpu)

    memory_stats = stats.get("memory_stats")
    memory_stats = memory_stats if isinstance(memory_stats, dict) else {}
    raw_memory = _integer(memory_stats.get("usage"))
    memory_details = memory_stats.get("stats")
    memory_details = memory_details if isinstance(memory_details, dict) else {}
    cached_memory = _integer(memory_details.get("inactive_file"))
    if cached_memory is None:
        cached_memory = _integer(memory_details.get("total_inactive_file"))
    working_set = max(0, raw_memory - cached_memory) if raw_memory is not None and cached_memory is not None else raw_memory

    networks = stats.get("networks")
    network_rx = _sum_counter_values(networks, "rx_bytes")
    network_tx = _sum_counter_values(networks, "tx_bytes")
    block_read, block_write = _block_counters(stats)

    return MetricsSample(
        container=container,
        timestamp=time.time() if timestamp is None else timestamp,
        cpu_total_ns=_integer(cpu_usage.get("total_usage")),
        system_cpu_total=_integer(cpu_stats.get("system_cpu_usage")),
        online_cpus=online_cpus,
        memory_usage_bytes=working_set,
        memory_raw_bytes=raw_memory,
        memory_limit_bytes=_integer(memory_stats.get("limit")),
        memory_peak_bytes=_integer(memory_stats.get("max_usage")),
        network_rx_bytes=network_rx,
        network_tx_bytes=network_tx,
        block_read_bytes=block_read,
        block_write_bytes=block_write,
    )


def _previous_value(previous: Mapping[str, Any] | MetricsSample, key: str) -> Any:
    if isinstance(previous, MetricsSample):
        return getattr(previous, key)
    return previous[key]


def _rate(current: int | None, previous: Any, elapsed: float) -> float | None:
    if current is None or previous is None or elapsed <= 0:
        return None
    delta = current - int(previous)
    return delta / elapsed if delta >= 0 else None


def derive_metrics(
    sample: MetricsSample,
    previous: Mapping[str, Any] | MetricsSample | None,
) -> dict[str, float | None]:
    derived: dict[str, float | None] = {field: None for field in DERIVED_FIELDS}
    if previous is None:
        return derived

    elapsed = sample.timestamp - float(_previous_value(previous, "timestamp"))
    if elapsed <= 0:
        return derived

    previous_cpu = _previous_value(previous, "cpu_total_ns")
    previous_system = _previous_value(previous, "system_cpu_total")
    if (
        sample.cpu_total_ns is not None
        and sample.system_cpu_total is not None
        and sample.online_cpus is not None
        and previous_cpu is not None
        and previous_system is not None
    ):
        cpu_delta = sample.cpu_total_ns - int(previous_cpu)
        system_delta = sample.system_cpu_total - int(previous_system)
        if cpu_delta >= 0 and system_delta > 0:
            derived["cpu_percent"] = 100.0 * cpu_delta / system_delta * sample.online_cpus

    for counter, rate_field in (
        ("network_rx_bytes", "network_rx_bytes_per_second"),
        ("network_tx_bytes", "network_tx_bytes_per_second"),
        ("block_read_bytes", "block_read_bytes_per_second"),
        ("block_write_bytes", "block_write_bytes_per_second"),
    ):
        derived[rate_field] = _rate(
            getattr(sample, counter),
            _previous_value(previous, counter),
            elapsed,
        )
    return derived


class SQLiteStore:
    def __init__(self, path: str | Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(self.path)
        self.connection.row_factory = sqlite3.Row
        self.connection.execute(
            """
            CREATE TABLE IF NOT EXISTS samples (
                id INTEGER PRIMARY KEY,
                container TEXT NOT NULL,
                timestamp REAL NOT NULL,
                cpu_total_ns INTEGER,
                system_cpu_total INTEGER,
                online_cpus INTEGER,
                memory_usage_bytes INTEGER,
                memory_raw_bytes INTEGER,
                memory_limit_bytes INTEGER,
                memory_peak_bytes INTEGER,
                network_rx_bytes INTEGER,
                network_tx_bytes INTEGER,
                block_read_bytes INTEGER,
                block_write_bytes INTEGER,
                cpu_percent REAL,
                network_rx_bytes_per_second REAL,
                network_tx_bytes_per_second REAL,
                block_read_bytes_per_second REAL,
                block_write_bytes_per_second REAL
            )
            """
        )
        self.connection.execute(
            "CREATE INDEX IF NOT EXISTS samples_container_timestamp ON samples(container, timestamp)"
        )
        self.connection.commit()

    def save(self, sample: MetricsSample) -> dict[str, float | None]:
        previous = self.connection.execute(
            "SELECT * FROM samples WHERE container = ? ORDER BY timestamp DESC, id DESC LIMIT 1",
            (sample.container,),
        ).fetchone()
        derived = derive_metrics(sample, previous)
        fields = ("container", "timestamp", *RAW_FIELDS, *DERIVED_FIELDS)
        values = (
            sample.container,
            sample.timestamp,
            *(getattr(sample, field) for field in RAW_FIELDS),
            *(derived[field] for field in DERIVED_FIELDS),
        )
        placeholders = ", ".join("?" for _ in fields)
        self.connection.execute(
            f"INSERT INTO samples ({', '.join(fields)}) VALUES ({placeholders})",
            values,
        )
        self.connection.commit()
        return derived

    def export_csv(
        self,
        output: TextIO,
        container: str | None = None,
        since: float | None = None,
    ) -> int:
        conditions = []
        parameters: list[Any] = []
        if container is not None:
            conditions.append("container = ?")
            parameters.append(container)
        if since is not None:
            conditions.append("timestamp >= ?")
            parameters.append(since)
        query = "SELECT * FROM samples"
        if conditions:
            query += " WHERE " + " AND ".join(conditions)
        query += " ORDER BY timestamp, id"
        rows = self.connection.execute(query, parameters)
        fields = ("container", "timestamp", *RAW_FIELDS, *DERIVED_FIELDS)
        writer = csv.writer(output)
        writer.writerow(("timestamp_utc", *fields[:1], *fields[2:]))
        count = 0
        for row in rows:
            timestamp_utc = datetime.fromtimestamp(row["timestamp"], timezone.utc).isoformat().replace("+00:00", "Z")
            writer.writerow((timestamp_utc, *(row[field] for field in fields if field != "timestamp")))
            count += 1
        return count

    def close(self) -> None:
        self.connection.close()

    def __enter__(self) -> SQLiteStore:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()


def connect_to_docker() -> Any:
    try:
        import docker
    except ImportError as error:
        raise EngineConnectionError(
            "The Docker SDK is not installed. Run 'python -m pip install -r requirements.txt'."
        ) from error

    client = None
    try:
        client = docker.from_env(timeout=5)
        client.ping()
        return client
    except docker.errors.DockerException as error:
        if client is not None:
            client.close()
        raise EngineConnectionError(
            "Cannot reach Docker Engine. Make it available to this environment (for example, "
            "through DOCKER_HOST or an accessible Docker socket), then try again."
        ) from error


def _format_bytes(value: int | float | None) -> str:
    if value is None:
        return "n/a"
    amount = float(value)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if abs(amount) < 1024 or unit == "TiB":
            return f"{amount:.1f} {unit}"
        amount /= 1024
    return f"{amount:.1f} TiB"


def _format_counter_rate(counter: int | None, rate: float | None) -> str:
    total = _format_bytes(counter)
    current_rate = f"{_format_bytes(rate)}/s" if rate is not None else "n/a"
    return f"{current_rate} ({total})"


def _render_table(rows: list[dict[str, Any]]) -> Any:
    from rich.table import Table

    table = Table(title="Docker container metrics", expand=True)
    table.add_column("Container", no_wrap=True)
    table.add_column("CPU", justify="right")
    table.add_column("Memory", justify="right")
    table.add_column("Mem %", justify="right")
    table.add_column("Network RX (rate / total)", justify="right")
    table.add_column("Network TX (rate / total)", justify="right")
    table.add_column("Block read (rate / total)", justify="right")
    table.add_column("Block write (rate / total)", justify="right")

    for row in rows:
        sample = row.get("sample")
        if sample is None:
            table.add_row(row["container"], "Unavailable", row["error"], "", "", "", "", "")
            continue
        memory_percent = None
        if sample.memory_usage_bytes is not None and sample.memory_limit_bytes:
            memory_percent = sample.memory_usage_bytes / sample.memory_limit_bytes * 100
        cpu_percent = row["derived"]["cpu_percent"]
        table.add_row(
            row["container"],
            f"{cpu_percent:.1f}%" if cpu_percent is not None else "n/a",
            f"{_format_bytes(sample.memory_usage_bytes)} / {_format_bytes(sample.memory_limit_bytes)}",
            f"{memory_percent:.1f}%" if memory_percent is not None else "n/a",
            _format_counter_rate(sample.network_rx_bytes, row["derived"]["network_rx_bytes_per_second"]),
            _format_counter_rate(sample.network_tx_bytes, row["derived"]["network_tx_bytes_per_second"]),
            _format_counter_rate(sample.block_read_bytes, row["derived"]["block_read_bytes_per_second"]),
            _format_counter_rate(sample.block_write_bytes, row["derived"]["block_write_bytes_per_second"]),
        )
    return table


def run_monitor(config: MonitorConfig, interval: float | None = None) -> None:
    from rich.console import Console
    from rich.live import Live

    poll_interval = config.poll_interval_seconds if interval is None else interval
    if not math.isfinite(poll_interval) or poll_interval <= 0:
        raise ConfigError("The polling interval must be a positive finite number.")

    client = connect_to_docker()
    console = Console()
    try:
        with SQLiteStore(config.database_path) as store, Live(
            _render_table([]), console=console, refresh_per_second=4
        ) as live:
            while True:
                poll_started = time.monotonic()
                rows = []
                for name in config.containers:
                    try:
                        container = client.containers.get(name)
                        stats = container.stats(stream=False)
                        sample = parse_stats(name, stats)
                        derived = store.save(sample)
                        rows.append({"container": name, "sample": sample, "derived": derived})
                    except Exception as error:
                        rows.append({"container": name, "error": str(error), "sample": None})
                live.update(_render_table(rows), refresh=True)
                delay = poll_interval - (time.monotonic() - poll_started)
                if delay > 0:
                    time.sleep(delay)
    finally:
        client.close()


def _positive_hours(value: str) -> float:
    try:
        hours = float(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError("must be a positive number") from error
    if not math.isfinite(hours) or hours <= 0:
        raise argparse.ArgumentTypeError("must be a positive number")
    return hours


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Monitor Docker container resource usage.")
    parser.add_argument("--config", type=Path, default=default_config_path(), help="path to containers.json")
    parser.add_argument("--interval", type=float, help="override the configured polling interval in seconds")
    commands = parser.add_subparsers(dest="command")
    export_parser = commands.add_parser("export", help="export stored samples to CSV")
    export_parser.add_argument("--container", help="export only this configured container")
    export_parser.add_argument("--since-hours", type=_positive_hours, help="include samples from the last N hours")
    export_parser.add_argument("--output", type=Path, help="CSV destination; defaults to standard output")
    args = parser.parse_args(argv)

    try:
        config = load_config(args.config, require_containers=args.command != "export")
        if args.command == "export":
            if args.container is not None and args.container not in config.containers:
                raise ConfigError(f"Container {args.container!r} is not listed in {args.config}.")
            since = time.time() - args.since_hours * 3600 if args.since_hours is not None else None
            with SQLiteStore(config.database_path) as store:
                if args.output is None:
                    count = store.export_csv(sys.stdout, container=args.container, since=since)
                else:
                    with args.output.open("w", encoding="utf-8", newline="") as output:
                        count = store.export_csv(output, container=args.container, since=since)
            if args.output is not None:
                print(f"Exported {count} samples to {args.output}.")
            return 0

        run_monitor(config, interval=args.interval)
    except (ConfigError, EngineConnectionError, OSError) as error:
        print(f"Error: {error}", file=sys.stderr)
        return 2
    except KeyboardInterrupt:
        print("Monitoring stopped.", file=sys.stderr)
        return 0
    return 0


if __name__ == "__main__":
    raise SystemExit(main())