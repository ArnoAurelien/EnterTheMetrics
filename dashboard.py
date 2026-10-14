from __future__ import annotations

import argparse
import sqlite3
import time
from pathlib import Path
from typing import Any

from flask import Flask, jsonify, render_template

from monitor import ConfigError, load_config


def _open_database(path: Path) -> sqlite3.Connection | None:
    if not path.is_file():
        return None
    connection = sqlite3.connect(f"{path.as_uri()}?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    return connection


def _tables(connection: sqlite3.Connection) -> set[str]:
    return {
        row["name"]
        for row in connection.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
    }


def _columns(connection: sqlite3.Connection, table: str) -> set[str]:
    return {row["name"] for row in connection.execute(f"PRAGMA table_info({table})")}


def _configured_match(token: str, container_id: str, name: str) -> bool:
    return token == name or token == container_id or container_id.startswith(token)


def _catalog(connection: sqlite3.Connection | None, configured: tuple[str, ...]) -> dict[str, list[dict[str, Any]]]:
    records: dict[str, dict[str, Any]] = {}
    legacy_names: set[str] = set()
    if connection is not None:
        tables = _tables(connection)
        if "sessions" in tables:
            for row in connection.execute("SELECT * FROM sessions ORDER BY started_at DESC, id DESC"):
                item = records.setdefault(
                    row["container_id"],
                    {
                        "id": row["container_id"],
                        "container_id": row["container_id"],
                        "name": row["container"],
                        "configured": False,
                        "latest_started_at": row["started_at"],
                        "status": row["status"],
                        "last_seen_at": row["last_seen_at"],
                    },
                )
                item["name"] = item["name"] or row["container"]
                if row["started_at"] >= item["latest_started_at"]:
                    item["latest_started_at"] = row["started_at"]
                    item["status"] = row["status"]
                    item["last_seen_at"] = row["last_seen_at"]
        if "samples" in tables:
            sample_columns = _columns(connection, "samples")
            session_filter = "session_id IS NULL AND " if "session_id" in sample_columns else ""
            legacy_names = {
                row["container"]
                for row in connection.execute(
                    f"SELECT DISTINCT container FROM samples WHERE {session_filter}container IS NOT NULL"
                )
            }

    configured_items: list[dict[str, Any]] = []
    matched_tokens: set[str] = set()
    for item in records.values():
        matches = [
            token
            for token in configured
            if _configured_match(token, item["container_id"], item["name"])
        ]
        if matches:
            item["configured"] = True
            matched_tokens.update(matches)
            configured_items.append(item)

    for token in configured:
        if token not in matched_tokens:
            configured_items.append(
                {
                    "id": f"configured:{token}",
                    "container_id": None,
                    "name": token,
                    "configured": True,
                    "status": "waiting",
                    "last_seen_at": None,
                }
            )

    archived = [item for item in records.values() if not item["configured"]]
    archived.extend(
        {
            "id": f"legacy:{name}",
            "container_id": None,
            "name": name,
            "configured": False,
            "status": "legacy history",
            "last_seen_at": None,
        }
        for name in sorted(legacy_names)
    )
    return {"configured": configured_items, "archived": archived}


def _query_samples(
    connection: sqlite3.Connection,
    container_key: str,
    session_id: int | None = None,
) -> list[dict[str, Any]]:
    tables = _tables(connection)
    columns = _columns(connection, "samples")
    gap = "COALESCE(samples.gap_detected, 0)" if "gap_detected" in columns else "0"
    interval = "samples.sample_interval_seconds" if "sample_interval_seconds" in columns else "NULL"
    session_column = "samples.session_id" if "session_id" in columns else "NULL"
    if session_id is not None:
        rows = connection.execute(
            f"SELECT samples.*, {gap} AS gap_detected_value, {interval} AS interval_value, "
            f"{session_column} AS session_id_value FROM samples WHERE samples.session_id = ? "
            "ORDER BY samples.timestamp, samples.id",
            (session_id,),
        )
    elif container_key.startswith("legacy:"):
        name = container_key.removeprefix("legacy:")
        legacy_filter = "session_id IS NULL AND " if "session_id" in columns else ""
        rows = connection.execute(
            f"SELECT samples.*, {gap} AS gap_detected_value, {interval} AS interval_value, "
            f"{session_column} AS session_id_value FROM samples WHERE {legacy_filter}container = ? "
            "ORDER BY samples.timestamp, samples.id",
            (name,),
        )
    elif "sessions" in tables:
        rows = connection.execute(
            f"SELECT samples.*, {gap} AS gap_detected_value, {interval} AS interval_value, "
            f"{session_column} AS session_id_value FROM samples "
            "JOIN sessions ON sessions.id = samples.session_id "
            "WHERE sessions.container_id = ? ORDER BY samples.timestamp, samples.id",
            (container_key,),
        )
    else:
        return []

    result = []
    for row in rows:
        data = dict(row)
        result.append(
            {
                "id": data.get("id"),
                "session_id": data.get("session_id_value"),
                "timestamp": data.get("timestamp"),
                "cpu_percent": data.get("cpu_percent"),
                "cpu_total_ns": data.get("cpu_total_ns"),
                "memory_usage_bytes": data.get("memory_usage_bytes"),
                "memory_limit_bytes": data.get("memory_limit_bytes"),
                "memory_peak_bytes": data.get("memory_peak_bytes"),
                "network_rx_bytes": data.get("network_rx_bytes"),
                "network_tx_bytes": data.get("network_tx_bytes"),
                "network_rx_bytes_per_second": data.get("network_rx_bytes_per_second"),
                "network_tx_bytes_per_second": data.get("network_tx_bytes_per_second"),
                "block_read_bytes": data.get("block_read_bytes"),
                "block_write_bytes": data.get("block_write_bytes"),
                "block_read_bytes_per_second": data.get("block_read_bytes_per_second"),
                "block_write_bytes_per_second": data.get("block_write_bytes_per_second"),
                "sample_interval_seconds": data.get("interval_value"),
                "gap_detected": bool(data.get("gap_detected_value")),
            }
        )
    return result


def _metric_totals(sample_groups: list[list[dict[str, Any]]]) -> dict[str, Any]:
    counter_fields = {
        "cpu_time_seconds": ("cpu_total_ns", 1_000_000_000),
        "network_received_bytes": ("network_rx_bytes", 1),
        "network_sent_bytes": ("network_tx_bytes", 1),
        "block_read_bytes": ("block_read_bytes", 1),
        "block_write_bytes": ("block_write_bytes", 1),
    }
    totals: dict[str, Any] = {}
    for output, (field, divisor) in counter_fields.items():
        values = [next((sample.get(field) for sample in reversed(group) if sample.get(field) is not None), None) for group in sample_groups if group]
        available = [value for value in values if value is not None]
        totals[output] = sum(available) / divisor if available else None
    peaks = [sample.get("memory_peak_bytes") for group in sample_groups for sample in group]
    known_peaks = [value for value in peaks if value is not None]
    totals["peak_memory_bytes"] = max(known_peaks) if known_peaks else None
    all_samples = [sample for group in sample_groups for sample in group]
    totals["sample_count"] = len(all_samples)
    totals["gap_count"] = sum(sample["gap_detected"] for sample in all_samples)
    totals["first_sample_at"] = min((sample["timestamp"] for sample in all_samples), default=None)
    totals["last_sample_at"] = max((sample["timestamp"] for sample in all_samples), default=None)
    return totals


def _session_payload(session: dict[str, Any], samples: list[dict[str, Any]], poll_interval: float) -> dict[str, Any]:
    totals = _metric_totals([samples])
    started_at = session["started_at"]
    startup_delay = session.get("startup_observation_delay_seconds") or 0
    startup_gap = startup_delay > poll_interval * 1.5
    if samples:
        samples[0]["gap_before"] = startup_gap
        samples[0]["gap_start"] = started_at
    gap_count = totals["gap_count"] + int(startup_gap)
    last_seen_at = session.get("last_seen_at")
    age = max(0, time.time() - last_seen_at) if last_seen_at is not None else None
    stale_after = max(poll_interval * 2.5, 10)
    status = session["status"]
    if status == "running" and age is not None and age > stale_after:
        status = "stale"
    return {
        **session,
        "started_at": started_at,
        "startup_gap": startup_gap,
        "gap_count": gap_count,
        "freshness_age_seconds": age,
        "status_display": status,
        "totals": totals,
        "samples": samples,
    }


def create_app(config_path: str | Path | None = None) -> Flask:
    app = Flask(__name__)
    config_file = Path(config_path) if config_path is not None else Path(__file__).with_name("containers.json")

    @app.get("/")
    def index():
        return render_template("dashboard.html")

    def dashboard_config():
        return load_config(config_file, require_containers=False)

    @app.get("/api/containers")
    def containers():
        try:
            config = dashboard_config()
        except ConfigError as error:
            return jsonify({"error": str(error)}), 400
        connection = _open_database(config.database_path)
        try:
            catalog = _catalog(connection, config.containers)
            for item in catalog["configured"] + catalog["archived"]:
                item["live_state"] = item["status"]
                if item["status"] == "running" and item.get("last_seen_at") is not None:
                    age = max(0, time.time() - item["last_seen_at"])
                    item["live_state"] = (
                        "live" if age <= max(config.poll_interval_seconds * 2.5, 10) else "stale"
                    )
            return jsonify({**catalog, "poll_interval_seconds": config.poll_interval_seconds})
        finally:
            if connection is not None:
                connection.close()

    @app.get("/api/containers/<path:container_key>")
    def container_detail(container_key: str):
        try:
            config = dashboard_config()
        except ConfigError as error:
            return jsonify({"error": str(error)}), 400
        connection = _open_database(config.database_path)
        if connection is None:
            item = next(
                (
                    container
                    for container in _catalog(None, config.containers)["configured"]
                    if container["id"] == container_key
                ),
                {"id": container_key, "name": container_key, "configured": True},
            )
            return jsonify(
                {
                    "container": item,
                    "sessions": [],
                    "samples": [],
                    "totals": _metric_totals([]),
                    "legacy": container_key.startswith("legacy:"),
                }
            )
        try:
            catalog = _catalog(connection, config.containers)
            item = next(
                (container for container in catalog["configured"] + catalog["archived"] if container["id"] == container_key),
                None,
            )
            if item is None:
                return jsonify({"error": "Container history not found."}), 404
            if container_key.startswith("legacy:"):
                samples = _query_samples(connection, container_key)
                return jsonify(
                    {
                        "container": item,
                        "sessions": [],
                        "samples": samples,
                        "totals": _metric_totals([samples]),
                        "legacy": True,
                    }
                )

            sessions = []
            groups = []
            if "sessions" in _tables(connection):
                session_rows = connection.execute(
                    "SELECT * FROM sessions WHERE container_id = ? ORDER BY started_at DESC, id DESC",
                    (container_key,),
                )
                for row in session_rows:
                    session = dict(row)
                    samples = _query_samples(connection, container_key, session["id"])
                    groups.append(samples)
                    sessions.append(_session_payload(session, samples, config.poll_interval_seconds))
            return jsonify(
                {
                    "container": item,
                    "sessions": sessions,
                    "samples": [sample for group in reversed(groups) for sample in group],
                    "totals": _metric_totals(groups),
                    "legacy": False,
                }
            )
        finally:
            connection.close()

    @app.get("/api/sessions/<int:session_id>")
    def session_detail(session_id: int):
        try:
            config = dashboard_config()
        except ConfigError as error:
            return jsonify({"error": str(error)}), 400
        connection = _open_database(config.database_path)
        if connection is None or "sessions" not in _tables(connection):
            if connection is not None:
                connection.close()
            return jsonify({"error": "Session history not found."}), 404
        try:
            row = connection.execute("SELECT * FROM sessions WHERE id = ?", (session_id,)).fetchone()
            if row is None:
                return jsonify({"error": "Session history not found."}), 404
            session = dict(row)
            samples = _query_samples(connection, session["container_id"], session_id)
            return jsonify(_session_payload(session, samples, config.poll_interval_seconds))
        finally:
            connection.close()

    return app


def main() -> None:
    parser = argparse.ArgumentParser(description="Open the Docker metrics dashboard.")
    parser.add_argument("--config", type=Path, default=Path(__file__).with_name("containers.json"))
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=5000)
    args = parser.parse_args()
    create_app(args.config).run(host=args.host, port=args.port, debug=False, threaded=True)


if __name__ == "__main__":
    main()