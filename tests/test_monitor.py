import csv
import io
import json
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from unittest.mock import patch

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


class CLITests(unittest.TestCase):
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