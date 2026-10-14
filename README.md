# Docker Metrics Monitor

This Python tool polls Docker Engine for resource statistics for the containers listed in `containers.json`. The monitor refreshes a terminal table and saves samples to SQLite; the read-only dashboard presents per-container and per-session history.

## Setup

Use Python 3.10 or newer. In the devcontainer, `.venv` is created and dependencies are installed automatically. If you need to install them manually, use the virtual environment's interpreter directly:

```sh
.venv/bin/python3 -m pip install -r requirements.txt
```

Edit `containers.json` and add the names or IDs of the containers to monitor:

```json
{
  "containers": ["api", "worker"],
  "poll_interval_seconds": 2,
  "database": "metrics.sqlite3"
}
```

The database path is resolved relative to the config file. Samples are retained indefinitely. The polling interval can also be overridden on the command line. The monitor rereads this config each poll, so removing a container stops new sampling while preserving its saved history.

## Docker Engine Access

The Docker Python SDK uses the Docker environment variables `DOCKER_HOST`, `DOCKER_TLS_VERIFY`, and `DOCKER_CERT_PATH`. The current devcontainer does not mount a Docker socket, so the Engine must be made accessible separately, for example through an already-configured secure remote endpoint or a socket mounted by the environment. The monitor does not install or configure a Docker daemon. If the Engine cannot be reached, it prints a setup-oriented error and exits.

## Run

Start live monitoring with the default config and interval:

```sh
.venv/bin/python3 monitor.py
```

Use a different config or polling interval like this:

```sh
.venv/bin/python3 monitor.py --config containers.json --interval 5
```

Each work session is identified by Docker's immutable container ID and `StartedAt` timestamp, not its creation time or configured name. The monitor records the first observation delay, each sample timestamp and interval, and flags intervals longer than 1.5 times the polling interval. The live table includes session start, sample time, interval, gap, and container state.

CPU usage, network bytes, and block I/O counters are stored as Docker reports them, together with derived CPU percentages and transfer rates. These counters can show totals already accumulated before monitoring began, while memory is only known at observed sample times. Per-session totals can use the final cumulative counter for each session; all-time totals can sum those session totals. No points are invented for an unmonitored interval, so rates over a flagged gap are averages and memory history has a gap. Docker may omit some metrics depending on the container/runtime; those values appear as `n/a`.

Sessions are marked finished when Docker reports a stop time. If monitoring was off during a restart, the previous session is marked `finished_inferred` with no fabricated finish time. If a container disappears, its last session remains identifiable by its last sample time rather than being falsely marked finished. A session that was never observed is not added to the database.

The SQLite database has a `sessions` table for container lifecycle metadata and a `samples` table linked by session ID. Existing sample rows are preserved during migration but remain unassigned to sessions because their original container IDs and start times were not recorded.

## Dashboard

Install the dependencies, then start the collector and dashboard in separate terminals:

```sh
.venv/bin/python3 monitor.py
.venv/bin/python3 dashboard.py
```

Open `http://127.0.0.1:5000`. The dashboard reads SQLite without modifying it and refreshes active views every three seconds; live data therefore appears only while the collector is running and Docker Engine is reachable. The left navigation lists configured containers and historical containers, including those removed from config. Container views show all-time totals and work sessions; session views show that session's metrics. Time-series charts leave breaks at recorded gaps and session boundaries. A late monitoring start is marked from the Docker start time and first observation. The chart library is loaded from jsDelivr, so chart rendering requires network access.

## Export History

Export all stored samples, or filter by container and time window:

```sh
.venv/bin/python3 monitor.py export --output history.csv
.venv/bin/python3 monitor.py export --container api --since-hours 24 --output api-last-day.csv
```

Without `--output`, CSV is written to standard output. The CSV includes UTC sample, session-start, and known session-finish times; session status and startup-observation delay; interval and gap fields; raw counters; memory measurements; CPU percentage; and derived transfer rates.