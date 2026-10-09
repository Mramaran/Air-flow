"""These tests need Airflow, so they run inside the demo container:

    docker compose exec airflow python -m pytest /opt/provider/tests/airflow -q

Each test talks to an in-process replay server. No key, no network, no searches.
"""

import json
import threading

import pytest

from replay.server import make_server

CONN_ID = "serpapi_test"


@pytest.fixture
def replay(monkeypatch):
    """Start a replay server and point the `serpapi_test` connection at it."""
    servers = []

    def start(processing_seconds=0.3, **kwargs):
        server = make_server(processing_seconds=processing_seconds, **kwargs)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        servers.append(server)
        monkeypatch.setenv(
            f"AIRFLOW_CONN_{CONN_ID.upper()}",
            json.dumps(
                {
                    "conn_type": "serpapi",
                    "password": "replay-key",
                    "host": f"http://127.0.0.1:{server.server_address[1]}",
                }
            ),
        )
        return server

    yield start
    for server in servers:
        server.shutdown()
        server.server_close()
