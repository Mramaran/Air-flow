"""Fail if anything that could be committed looks like a SerpApi key."""

import os
import re
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
KEY_SHAPE = re.compile(r"(?<![0-9a-f])[0-9a-f]{64}(?![0-9a-f])")


def candidate_files():
    """Tracked files plus untracked files that are not git-ignored."""
    listed = subprocess.run(
        ["git", "ls-files", "--cached", "--others", "--exclude-standard"],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=True,
    ).stdout.splitlines()
    return [ROOT / name for name in listed if (ROOT / name).is_file()]


def read(path):
    try:
        return path.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        return ""


def test_env_file_is_ignored_by_git():
    ignored = subprocess.run(["git", "check-ignore", ".env"], cwd=ROOT, capture_output=True, text=True)

    assert ignored.returncode == 0, ".env must be git-ignored"


def test_no_file_contains_a_string_shaped_like_a_serpapi_key():
    offenders = [str(path.relative_to(ROOT)) for path in candidate_files() if KEY_SHAPE.search(read(path))]

    assert offenders == []


def test_no_file_contains_the_real_key_when_one_is_set():
    key = os.environ.get("SERPAPI_KEY", "").strip()
    env_file = ROOT / ".env"
    if not key and env_file.is_file():
        for line in read(env_file).splitlines():
            name, _, value = line.partition("=")
            if name.strip() == "SERPAPI_KEY":
                key = value.strip().strip('"').strip("'")
    if not key:
        return

    offenders = [str(path.relative_to(ROOT)) for path in candidate_files() if key in read(path)]

    assert offenders == []
