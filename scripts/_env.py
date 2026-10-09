"""Read SERPAPI_KEY from the environment or from .env. The key is never printed."""

from __future__ import annotations

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


def load_key() -> str:
    key = os.environ.get("SERPAPI_KEY", "").strip()
    env_file = ROOT / ".env"
    if not key and env_file.is_file():
        for line in env_file.read_text(encoding="utf-8").splitlines():
            name, _, value = line.partition("=")
            if name.strip() == "SERPAPI_KEY":
                key = value.strip().strip('"').strip("'")
    if not key:
        sys.exit("No SerpApi key found. Copy .env.example to .env and paste your key after SERPAPI_KEY=.")
    return key


def make_client():
    """A client for the real SerpApi, or for SERPAPI_BASE_URL when that is set (used to test the scripts)."""
    from serpapi_provider.core.client import DEFAULT_BASE_URL, SerpApiClient

    return SerpApiClient(load_key(), base_url=os.environ.get("SERPAPI_BASE_URL", DEFAULT_BASE_URL))
