import csv
import io
import json
import sqlite3
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from unittest.mock import patch
from rich.console import Console

from monitor import (
    ConfigError,
    EngineConnectionError,
    MetricsSample,
    SQLiteStore,
    connect_to_docker,
    derive_metrics,
    load_config,
    main,
    parse_stats,
    _render_table,
)


class ConfigTests(unittest.TestCase):
    def test_loads_containers_and_resolves_database_relative_to_config(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            config_path = Path(temporary_directory) / "containers.json"
            config_path.write_text(
                json.dumps({"containers": [" api ", "worker"], "database": "data/metrics.db"}),
                encoding="utf-8",
            )

            config = load_config(config_path)

            self.assertEqual(config.containers, ("api", "worker"))
            self.assertEqual(config.poll_interval_seconds, 2)
            self.assertEqual(config.database_path, Path(temporary_directory) / "data/metrics.db")

    def test_rejects_empty_container_list(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            config_path = Path(temporary_directory) / "containers.json"
            config_path.write_text('{"containers": []}', encoding="utf-8")

            with self.assertRaisesRegex(ConfigError, "cannot be empty for monitoring"):
                load_config(config_path)


class StatsTests(unittest.TestCase):
    def test_parses_cpu_memory_network_and_block_io(self):
        sample = parse_stats(
            "api",
            {
                "cpu_stats": {
                    "cpu_usage": {"total_usage": 500, "percpu_usage": [1, 2]},
                    "system_cpu_usage": 5000,
                },
                "memory_stats": {
                    "usage": 900,
                    "limit": 2000,
                    "max_usage": 1200,
                    "stats": {"inactive_file": 300},
                },
                "networks": {
                    "eth0": {"rx_bytes": 100, "tx_bytes": 200},
                    "eth1": {"rx_bytes": 50, "tx_bytes": 75},
                },
                "blkio_stats": {
                    "io_service_bytes_recursive": [
                        {"op": "Read", "value": 30},
                        {"op": "Write", "value": 40},
                        {"op": "Total", "value": 70},
                    ]
                },
            },
            timestamp=10,
        )

        self.assertEqual(sample.cpu_total_ns, 500)
        self.assertEqual(sample.online_cpus, 2)
        self.assertEqual(sample.memory_usage_bytes, 600)
        self.assertEqual(sample.memory_raw_bytes, 900)
        self.assertEqual(sample.memory_peak_bytes, 1200)
        self.assertEqual(sample.network_rx_bytes, 150)
        self.assertEqual(sample.network_tx_bytes, 275)
        self.assertEqual(sample.block_read_bytes, 30)
        self.assertEqual(sample.block_write_bytes, 40)

    def test_missing_optional_stats_are_none(self):
        sample = parse_stats("api", {}, timestamp=10)

        self.assertIsNone(sample.cpu_total_ns)
        self.assertIsNone(sample.memory_usage_bytes)
        self.assertIsNone(sample.network_rx_bytes)
        self.assertIsNone(sample.block_read_bytes)

    def test_parses_container_id_and_docker_started_at(self):
        sample = parse_stats(
            "api",
            {},
            timestamp=100,
            container_id="container-123",
            started_at="1970-01-01T00:01:30.000000000Z",
        )

        self.assertEqual(sample.container_id, "container-123")
        self.assertEqual(sample.session_started_at, 90)

    def test_derives_cpu_percentage_and_io_rates(self):
        previous = MetricsSample(
            container="api",
            timestamp=10,
            cpu_total_ns=100,
            system_cpu_total=1000,
            network_rx_bytes=1000,
            network_tx_bytes=2000,
        )
        current = MetricsSample(
            container="api",
            timestamp=12,
            cpu_total_ns=300,
            system_cpu_total=1500,
            online_cpus=4,
            network_rx_bytes=1400,
            network_tx_bytes=2200,
        )

        derived = derive_metrics(current, previous)

        self.assertEqual(derived["cpu_percent"], 160)
        self.assertEqual(derived["network_rx_bytes_per_second"], 200)
        self.assertEqual(derived["network_tx_bytes_per_second"], 100)
        self.assertIsNone(derived["block_read_bytes_per_second"])

    def test_counter_reset_does_not_produce_negative_rates(self):
        previous = MetricsSample(container="api", timestamp=10, network_rx_bytes=1000)
        current = MetricsSample(container="api", timestamp=11, network_rx_bytes=20)

        self.assertIsNone(derive_metrics(current, previous)["network_rx_bytes_per_second"])


class StoreTests(unittest.TestCase):
    def test_persists_derived_metrics_and_exports_csv(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            database = Path(temporary_directory) / "metrics.sqlite3"
            first = MetricsSample(
                container="api",
                timestamp=10,
                cpu_total_ns=100,
                system_cpu_total=1000,
                online_cpus=2,
                network_rx_bytes=100,
            )
            second = MetricsSample(
                container="api",
                timestamp=12,
                cpu_total_ns=300,
                system_cpu_total=1500,
                online_cpus=2,
                network_rx_bytes=500,
            )

            with SQLiteStore(database) as store:
                store.save(first)
                derived = store.save(second)
                output = io.StringIO()
                count = store.export_csv(output, container="api", since=11)

            rows = list(csv.DictReader(io.StringIO(output.getvalue())))

        self.assertEqual(derived["cpu_percent"], 80)
        self.assertEqual(derived["network_rx_bytes_per_second"], 200)
        self.assertEqual(count, 1)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["container"], "api")
        self.assertEqual(rows[0]["network_rx_bytes"], "500")
        self.assertEqual(rows[0]["network_rx_bytes_per_second"], "200.0")

    def test_tracks_session_start_delay_gaps_and_finish_in_csv(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            database = Path(temporary_directory) / "metrics.sqlite3"
            with SQLiteStore(database) as store:
                session_id = store.begin_session(
                    container_id="container-123",
                    container="api",
                    started_at=90,
                    observed_at=100,
                    poll_interval_seconds=2,
                )
                first = MetricsSample(
                    container="api",
                    container_id="container-123",
                    session_started_at=90,
                    timestamp=100,
                    network_rx_bytes=100,
                )
                first_metrics = store.save(first, session_id=session_id, poll_interval_seconds=2)
                second = MetricsSample(
                    container="api",
                    container_id="container-123",
                    session_started_at=90,
                    timestamp=1000,
                    network_rx_bytes=1900,
                )
                second_metrics = store.save(second, session_id=session_id, poll_interval_seconds=2)
                store.finish_container_session("container-123", 90, finished_at=1001)
                output = io.StringIO()
                count = store.export_csv(output, container="api")
                session = store.list_sessions(container_id="container-123")[0]

            rows = list(csv.DictReader(io.StringIO(output.getvalue())))

        self.assertIsNone(first_metrics["sample_interval_seconds"])
        self.assertEqual(second_metrics["sample_interval_seconds"], 900)
        self.assertTrue(second_metrics["gap_detected"])
        self.assertEqual(session["status"], "finished")
        self.assertEqual(session["startup_observation_delay_seconds"], 10)
        self.assertEqual(count, 2)
        self.assertEqual(rows[0]["session_started_at_utc"], "1970-01-01T00:01:30Z")
        self.assertEqual(rows[0]["session_finished_at_utc"], "1970-01-01T00:16:41Z")
        self.assertEqual(rows[0]["startup_observation_delay_seconds"], "10.0")
        self.assertEqual(rows[1]["sample_interval_seconds"], "900.0")
        self.assertEqual(rows[1]["gap_detected"], "1")
        self.assertEqual(rows[1]["session_status"], "finished")

    def test_new_start_infers_previous_session_finished(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            with SQLiteStore(Path(temporary_directory) / "metrics.sqlite3") as store:
                previous_id = store.begin_session("container-123", "api", 10, 11, 2)
                current_id = store.begin_session("container-123", "api", 30, 31, 2)
                sessions = store.list_sessions(container_id="container-123")

        self.assertNotEqual(previous_id, current_id)
        self.assertEqual(sessions[0]["id"], current_id)
        self.assertEqual(sessions[0]["status"], "running")
        self.assertEqual(sessions[1]["id"], previous_id)
        self.assertEqual(sessions[1]["status"], "finished_inferred")
        self.assertIsNone(sessions[1]["finished_at"])

    def test_migrates_existing_samples_without_losing_rows(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            database = Path(temporary_directory) / "metrics.sqlite3"
            with sqlite3.connect(database) as connection:
                connection.execute(
                    """
                    CREATE TABLE samples (
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
                connection.execute(
                    "INSERT INTO samples (container, timestamp, network_rx_bytes) VALUES (?, ?, ?)",
                    ("api", 100, 50),
                )

            with SQLiteStore(database) as store:
                row = store.connection.execute("SELECT * FROM samples").fetchone()
                columns = {column["name"] for column in store.connection.execute("PRAGMA table_info(samples)")}

        self.assertEqual(row["network_rx_bytes"], 50)
        self.assertIsNone(row["session_id"])
        self.assertTrue({"session_id", "sample_interval_seconds", "gap_detected"}.issubset(columns))


class CLITests(unittest.TestCase):
    def test_live_table_has_consistent_state_column_for_stopped_container(self):
        output = io.StringIO()
        table = _render_table(
            [
                {
                    "container": "api",
                    "sample": None,
                    "started_at": 10,
                    "sampled_at": 20,
                    "status": "exited: no current sample",
                }
            ]
        )
        Console(file=output, width=180).print(table)

        self.assertIn("State", output.getvalue())
        self.assertIn("exited:", output.getvalue())

    def test_export_works_with_empty_container_list(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            config_path = Path(temporary_directory) / "containers.json"
            config_path.write_text(
                json.dumps({"containers": [], "database": "metrics.sqlite3"}),
                encoding="utf-8",
            )
            output = io.StringIO()

            with redirect_stdout(output):
                result = main(["--config", str(config_path), "export"])

        self.assertEqual(result, 0)
        self.assertIn("timestamp_utc,container", output.getvalue())

    def test_monitor_reports_unreachable_engine(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            config_path = Path(temporary_directory) / "containers.json"
            config_path.write_text('{"containers": ["api"]}', encoding="utf-8")
            error_output = io.StringIO()

            with patch("monitor.connect_to_docker", side_effect=EngineConnectionError("Engine unavailable")):
                with redirect_stderr(error_output):
                    result = main(["--config", str(config_path)])

        self.assertEqual(result, 2)
        self.assertIn("Engine unavailable", error_output.getvalue())

    def test_docker_sdk_connection_error_has_actionable_message(self):
        import docker

        with patch("docker.from_env", side_effect=docker.errors.DockerException("socket unavailable")):
            with self.assertRaisesRegex(EngineConnectionError, "Cannot reach Docker Engine"):
                connect_to_docker()


if __name__ == "__main__":
    unittest.main()