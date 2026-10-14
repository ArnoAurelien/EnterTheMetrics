# Docker Metrics Monitor

This Python CLI polls Docker Engine for resource statistics for the containers listed in `containers.json`. It refreshes a terminal table and saves samples to SQLite so they can be exported for plotting.

## Setup

Use Python 3.10 or newer. In the devcontainer, `.venv` is created and dependencies are installed automatically. Activate it in a terminal:

```sh
. .venv/bin/activate
python -m pip install -r requirements.txt
```

Edit `containers.json` and add the names or IDs of the containers to monitor:

```json
{
  "containers": ["api", "worker"],
  "poll_interval_seconds": 2,
  "database": "metrics.sqlite3"
}
```

The database path is resolved relative to the config file. Samples are retained indefinitely. The polling interval can also be overridden on the command line.

## Docker Engine Access

The Docker Python SDK uses the Docker environment variables `DOCKER_HOST`, `DOCKER_TLS_VERIFY`, and `DOCKER_CERT_PATH`. The current devcontainer does not mount a Docker socket, so the Engine must be made accessible separately, for example through an already-configured secure remote endpoint or a socket mounted by the environment. The monitor does not install or configure a Docker daemon. If the Engine cannot be reached, it prints a setup-oriented error and exits.

## Run

Start live monitoring with the default config and interval:

```sh
python monitor.py
```

Use a different config or polling interval like this:

```sh
python monitor.py --config containers.json --interval 5
```

The table reports CPU percentage, working-set memory and limit, memory percentage, network transfer rates and cumulative bytes, and block I/O rates and cumulative bytes. CPU percentage can exceed 100% when a container uses multiple cores. The first sample has no rate because there is no prior counter to compare with. Docker may omit some metrics depending on the container/runtime; those values appear as `n/a`.

## Export History

Export all stored samples, or filter by container and time window:

```sh
python monitor.py export --output history.csv
python monitor.py export --container api --since-hours 24 --output api-last-day.csv
```

Without `--output`, CSV is written to standard output. The CSV includes UTC timestamps, raw counters, memory measurements, CPU percentage, and derived transfer rates.