import json
import tempfile
import unittest
from pathlib import Path

from dashboard import create_app
from monitor import MetricsSample, SQLiteStore


class DashboardTests(unittest.TestCase):
    def setUp(self):
        self.temporary_directory = tempfile.TemporaryDirectory()
        root = Path(self.temporary_directory.name)
        self.config_path = root / "containers.json"
        self.database_path = root / "metrics.sqlite3"
        self.config_path.write_text(
            json.dumps(
                {
                    "containers": ["api"],
                    "poll_interval_seconds": 2,
                    "database": str(self.database_path),
                }
            ),
            encoding="utf-8",
        )
        with SQLiteStore(self.database_path) as store:
            session_id = store.begin_session("container-123", "api", 90, 100, 2)
            store.save(
                MetricsSample(
                    container="api",
                    container_id="container-123",
                    session_started_at=90,
                    timestamp=100,
                    cpu_total_ns=1_000_000_000,
                    memory_usage_bytes=100,
                ),
                session_id,
                2,
            )
            store.save(
                MetricsSample(
                    container="api",
                    container_id="container-123",
                    session_started_at=90,
                    timestamp=110,
                    cpu_total_ns=2_000_000_000,
                    memory_usage_bytes=200,
                    network_rx_bytes=500,
                ),
                session_id,
                2,
            )
            store.finish_session(session_id, 120)

            archived_id = store.begin_session("old-container", "old-worker", 50, 55, 2)
            store.save(
                MetricsSample(
                    container="old-worker",
                    container_id="old-container",
                    session_started_at=50,
                    timestamp=55,
                    cpu_total_ns=500_000_000,
                ),
                archived_id,
                2,
            )

        self.client = create_app(self.config_path).test_client()

    def tearDown(self):
        self.temporary_directory.cleanup()

    def test_catalog_includes_configured_and_archived_containers(self):
        response = self.client.get("/api/containers")
        data = response.get_json()

        self.assertEqual(response.status_code, 200)
        self.assertEqual(data["configured"][0]["container_id"], "container-123")
        self.assertEqual(data["archived"][0]["container_id"], "old-container")

    def test_container_overview_aggregates_per_session_counters(self):
        response = self.client.get("/api/containers/container-123")
        data = response.get_json()

        self.assertEqual(response.status_code, 200)
        self.assertEqual(data["totals"]["cpu_time_seconds"], 2)
        self.assertEqual(data["totals"]["sample_count"], 2)
        self.assertEqual(len(data["sessions"]), 1)

    def test_session_endpoint_preserves_sample_timestamps_and_gaps(self):
        overview = self.client.get("/api/containers/container-123").get_json()
        session_id = overview["sessions"][0]["id"]

        response = self.client.get(f"/api/sessions/{session_id}")
        data = response.get_json()

        self.assertEqual(response.status_code, 200)
        self.assertEqual(data["samples"][0]["timestamp"], 100)
        self.assertIsNone(data["samples"][0]["cpu_percent"])

    def test_removed_config_container_remains_available_from_history(self):
        self.config_path.write_text(
            json.dumps({"containers": [], "database": str(self.database_path)}),
            encoding="utf-8",
        )

        catalog = self.client.get("/api/containers").get_json()

        self.assertEqual(catalog["configured"], [])
        self.assertEqual(
            {item["container_id"] for item in catalog["archived"]},
            {"container-123", "old-container"},
        )


if __name__ == "__main__":
    unittest.main()