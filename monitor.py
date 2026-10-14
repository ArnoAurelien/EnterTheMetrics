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
    container_id: str | None = None
    session_started_at: float | None = None
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


def parse_docker_timestamp(value: Any) -> float | None:
    if not isinstance(value, str) or not value or value.startswith("0001-01-01T00:00:00"):
        return None
    normalized = value[:-1] + "+00:00" if value.endswith("Z") else value
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.timestamp()


def parse_stats(
    container: str,
    stats: Mapping[str, Any],
    timestamp: float | None = None,
    container_id: str | None = None,
    started_at: str | None = None,
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
        container_id=container_id,
        session_started_at=parse_docker_timestamp(started_at),
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
    poll_interval_seconds: float = 2,
) -> dict[str, float | bool | None]:
    derived: dict[str, float | bool | None] = {field: None for field in DERIVED_FIELDS}
    derived["sample_interval_seconds"] = None
    derived["gap_detected"] = False
    if previous is None:
        return derived

    elapsed = sample.timestamp - float(_previous_value(previous, "timestamp"))
    derived["sample_interval_seconds"] = elapsed if elapsed > 0 else None
    derived["gap_detected"] = elapsed > poll_interval_seconds * 1.5
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
            CREATE TABLE IF NOT EXISTS sessions (
                id INTEGER PRIMARY KEY,
                container_id TEXT NOT NULL,
                container TEXT NOT NULL,
                started_at REAL NOT NULL,
                first_observed_at REAL NOT NULL,
                startup_observation_delay_seconds REAL NOT NULL,
                status TEXT NOT NULL,
                finished_at REAL,
                last_seen_at REAL NOT NULL,
                UNIQUE(container_id, started_at)
            )
            """
        )
        self.connection.execute(
            """
            CREATE TABLE IF NOT EXISTS samples (
                id INTEGER PRIMARY KEY,
                container TEXT NOT NULL,
                session_id INTEGER REFERENCES sessions(id),
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
                block_write_bytes_per_second REAL,
                sample_interval_seconds REAL,
                gap_detected INTEGER
            )
            """
        )
        self._migrate_samples()
        self.connection.execute(
            "CREATE INDEX IF NOT EXISTS samples_container_timestamp ON samples(container, timestamp)"
        )
        self.connection.execute(
            "CREATE INDEX IF NOT EXISTS samples_session_timestamp ON samples(session_id, timestamp)"
        )
        self.connection.execute(
            "CREATE INDEX IF NOT EXISTS sessions_container_started ON sessions(container_id, started_at)"
        )
        self.connection.commit()

    def _migrate_samples(self) -> None:
        columns = {row["name"] for row in self.connection.execute("PRAGMA table_info(samples)")}
        migrations = {
            "session_id": "INTEGER REFERENCES sessions(id)",
            "sample_interval_seconds": "REAL",
            "gap_detected": "INTEGER",
        }
        for name, column_type in migrations.items():
            if name not in columns:
                self.connection.execute(f"ALTER TABLE samples ADD COLUMN {name} {column_type}")

    def begin_session(
        self,
        container_id: str,
        container: str,
        started_at: float,
        observed_at: float,
        poll_interval_seconds: float,
    ) -> int:
        self.connection.execute(
            """
            UPDATE sessions
            SET status = 'finished_inferred'
            WHERE container_id = ? AND status = 'running' AND started_at <> ?
            """,
            (container_id, started_at),
        )
        existing = self.connection.execute(
            "SELECT id FROM sessions WHERE container_id = ? AND started_at = ?",
            (container_id, started_at),
        ).fetchone()
        if existing is None:
            cursor = self.connection.execute(
                """
                INSERT INTO sessions (
                    container_id, container, started_at, first_observed_at,
                    startup_observation_delay_seconds, status, last_seen_at
                ) VALUES (?, ?, ?, ?, ?, 'running', ?)
                """,
                (
                    container_id,
                    container,
                    started_at,
                    observed_at,
                    max(0, observed_at - started_at),
                    observed_at,
                ),
            )
            session_id = int(cursor.lastrowid)
        else:
            session_id = int(existing["id"])
            self.connection.execute(
                """
                UPDATE sessions SET container = ?, status = 'running', finished_at = NULL,
                    last_seen_at = ? WHERE id = ?
                """,
                (container, observed_at, session_id),
            )
        self.connection.commit()
        return session_id

    def finish_session(self, session_id: int, finished_at: float | None = None) -> None:
        status = "finished" if finished_at is not None else "finished_inferred"
        self.connection.execute(
            "UPDATE sessions SET status = ?, finished_at = ? WHERE id = ?",
            (status, finished_at, session_id),
        )
        self.connection.commit()

    def finish_container_session(
        self,
        container_id: str,
        started_at: float,
        finished_at: float | None,
    ) -> None:
        session = self.connection.execute(
            "SELECT id FROM sessions WHERE container_id = ? AND started_at = ?",
            (container_id, started_at),
        ).fetchone()
        if session is not None:
            self.finish_session(int(session["id"]), finished_at)

    def list_sessions(self, container_id: str | None = None) -> list[dict[str, Any]]:
        query = "SELECT * FROM sessions"
        parameters: tuple[Any, ...] = ()
        if container_id is not None:
            query += " WHERE container_id = ?"
            parameters = (container_id,)
        query += " ORDER BY started_at DESC, id DESC"
        return [dict(row) for row in self.connection.execute(query, parameters)]

    def get_session(self, session_id: int) -> dict[str, Any] | None:
        row = self.connection.execute("SELECT * FROM sessions WHERE id = ?", (session_id,)).fetchone()
        return dict(row) if row is not None else None

    def save(
        self,
        sample: MetricsSample,
        session_id: int | None = None,
        poll_interval_seconds: float = 2,
    ) -> dict[str, float | bool | None]:
        if session_id is None:
            previous = self.connection.execute(
                """
                SELECT * FROM samples WHERE container = ? AND session_id IS NULL
                ORDER BY timestamp DESC, id DESC LIMIT 1
                """,
                (sample.container,),
            ).fetchone()
        else:
            previous = self.connection.execute(
                "SELECT * FROM samples WHERE session_id = ? ORDER BY timestamp DESC, id DESC LIMIT 1",
                (session_id,),
            ).fetchone()
        derived = derive_metrics(sample, previous, poll_interval_seconds)
        fields = (
            "container",
            "session_id",
            "timestamp",
            *RAW_FIELDS,
            *DERIVED_FIELDS,
            "sample_interval_seconds",
            "gap_detected",
        )
        values = (
            sample.container,
            session_id,
            sample.timestamp,
            *(getattr(sample, field) for field in RAW_FIELDS),
            *(derived[field] for field in DERIVED_FIELDS),
            derived["sample_interval_seconds"],
            derived["gap_detected"],
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
            conditions.append("samples.container = ?")
            parameters.append(container)
        if since is not None:
            conditions.append("samples.timestamp >= ?")
            parameters.append(since)
        query = """
            SELECT samples.*, sessions.container_id AS session_container_id,
                sessions.started_at AS session_started_at,
                sessions.finished_at AS session_finished_at,
                sessions.startup_observation_delay_seconds AS startup_observation_delay_seconds,
                sessions.status AS session_status
            FROM samples LEFT JOIN sessions ON sessions.id = samples.session_id
        """
        if conditions:
            query += " WHERE " + " AND ".join(conditions)
        query += " ORDER BY samples.timestamp, samples.id"
        rows = self.connection.execute(query, parameters)
        writer = csv.writer(output)
        writer.writerow(
            (
                "timestamp_utc",
                "container",
                "container_id",
                "session_started_at_utc",
                "session_finished_at_utc",
                "session_status",
                "startup_observation_delay_seconds",
                "sample_interval_seconds",
                "gap_detected",
                *RAW_FIELDS,
                *DERIVED_FIELDS,
            )
        )
        count = 0
        for row in rows:
            timestamp_utc = datetime.fromtimestamp(row["timestamp"], timezone.utc).isoformat().replace("+00:00", "Z")
            session_started_at = row["session_started_at"]
            session_started_at_utc = (
                datetime.fromtimestamp(session_started_at, timezone.utc).isoformat().replace("+00:00", "Z")
                if session_started_at is not None
                else ""
            )
            session_finished_at = row["session_finished_at"]
            session_finished_at_utc = (
                datetime.fromtimestamp(session_finished_at, timezone.utc).isoformat().replace("+00:00", "Z")
                if session_finished_at is not None
                else ""
            )
            writer.writerow(
                (
                    timestamp_utc,
                    row["container"],
                    row["session_container_id"] or "",
                    session_started_at_utc,
                    session_finished_at_utc,
                    row["session_status"] or "unknown",
                    row["startup_observation_delay_seconds"],
                    row["sample_interval_seconds"],
                    row["gap_detected"],
                    *(row[field] for field in RAW_FIELDS),
                    *(row[field] for field in DERIVED_FIELDS),
                )
            )
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


def _format_timestamp(timestamp: float | None) -> str:
    if timestamp is None:
        return "unknown"
    return datetime.fromtimestamp(timestamp, timezone.utc).strftime("%Y-%m-%d %H:%M:%SZ")


def _render_table(rows: list[dict[str, Any]]) -> Any:
    from rich.table import Table

    table = Table(title="Docker container metrics", expand=True)
    table.add_column("Container", no_wrap=True)
    table.add_column("Session started (UTC)")
    table.add_column("Sampled (UTC)")
    table.add_column("Δt")
    table.add_column("Gap")
    table.add_column("CPU", justify="right")
    table.add_column("Memory", justify="right")
    table.add_column("Mem %", justify="right")
    table.add_column("Network RX (rate / total)", justify="right")
    table.add_column("Network TX (rate / total)", justify="right")
    table.add_column("Block read (rate / total)", justify="right")
    table.add_column("Block write (rate / total)", justify="right")
    table.add_column("State")

    for row in rows:
        sample = row.get("sample")
        if sample is None:
            table.add_row(
                row["container"],
                _format_timestamp(row.get("started_at")),
                _format_timestamp(row.get("sampled_at")),
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                row.get("status", row.get("error", "Unavailable")),
            )
            continue
        memory_percent = None
        if sample.memory_usage_bytes is not None and sample.memory_limit_bytes:
            memory_percent = sample.memory_usage_bytes / sample.memory_limit_bytes * 100
        cpu_percent = row["derived"]["cpu_percent"]
        interval = row["derived"]["sample_interval_seconds"]
        if interval is None:
            startup_delay = row["session"]["startup_observation_delay_seconds"]
            interval_label = f"first +{startup_delay:.1f}s"
            gap_label = "startup gap" if startup_delay > row["poll_interval_seconds"] * 1.5 else "baseline"
        else:
            interval_label = f"{interval:.1f}s"
            gap_label = "GAP" if row["derived"]["gap_detected"] else "no"
        table.add_row(
            row["container"],
            _format_timestamp(sample.session_started_at),
            _format_timestamp(sample.timestamp),
            interval_label,
            gap_label,
            f"{cpu_percent:.1f}%" if cpu_percent is not None else "n/a",
            f"{_format_bytes(sample.memory_usage_bytes)} / {_format_bytes(sample.memory_limit_bytes)}",
            f"{memory_percent:.1f}%" if memory_percent is not None else "n/a",
            _format_counter_rate(sample.network_rx_bytes, row["derived"]["network_rx_bytes_per_second"]),
            _format_counter_rate(sample.network_tx_bytes, row["derived"]["network_tx_bytes_per_second"]),
            _format_counter_rate(sample.block_read_bytes, row["derived"]["block_read_bytes_per_second"]),
            _format_counter_rate(sample.block_write_bytes, row["derived"]["block_write_bytes_per_second"]),
            row.get("status", "running"),
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
                        container.reload()
                        attributes = container.attrs
                        state = attributes.get("State", {})
                        status = state.get("Status", "unknown")
                        container_id = attributes.get("Id")
                        started_at_text = state.get("StartedAt")
                        started_at = parse_docker_timestamp(started_at_text)
                        if not container_id or started_at is None:
                            raise ValueError("Docker did not provide a valid container ID and StartedAt timestamp.")
                        if status != "running":
                            store.finish_container_session(
                                container_id,
                                started_at,
                                parse_docker_timestamp(state.get("FinishedAt")),
                            )
                            rows.append(
                                {
                                    "container": name,
                                    "started_at": started_at,
                                    "sampled_at": time.time(),
                                    "status": f"{status}: no current sample",
                                    "sample": None,
                                }
                            )
                            continue
                        stats = container.stats(stream=False)
                        sample = parse_stats(
                            name,
                            stats,
                            container_id=container_id,
                            started_at=started_at_text,
                        )
                        session_id = store.begin_session(
                            container_id,
                            name,
                            started_at,
                            sample.timestamp,
                            poll_interval,
                        )
                        derived = store.save(sample, session_id, poll_interval)
                        rows.append(
                            {
                                "container": name,
                                "sample": sample,
                                "derived": derived,
                                "session": store.get_session(session_id),
                                "poll_interval_seconds": poll_interval,
                            }
                        )
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