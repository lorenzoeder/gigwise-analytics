"""@bruin
name: orchestration.run_dlt_ingestion
type: python
@bruin"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path


def main() -> None:
    repo_root = Path(__file__).resolve().parents[3]
    script = repo_root / "dlt" / "ingest_pipeline.py"

    env = dict(os.environ)
    env["PYTHONUNBUFFERED"] = "1"

    cmd = [sys.executable, str(script)]
    subprocess.run(cmd, check=True, cwd=repo_root, env=env)


if __name__ == "__main__":
    main()
