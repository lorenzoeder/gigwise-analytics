"""@bruin
name: transformation.run_dbt_build
type: python
depends:
  - quality.check_event_dates
@bruin"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path


def _run(cmd: list[str], cwd: Path, env: dict[str, str]) -> None:
    subprocess.run(cmd, check=True, cwd=cwd, env=env)


def _fix_dbt_packages_ownership(dbt_dir: Path) -> None:
    """Make dbt_packages world-writable so both root (Kestra) and local user can manage it."""
    pkgs = dbt_dir / "dbt_packages"
    if pkgs.exists() and os.getuid() == 0:
        subprocess.run(["chmod", "-R", "a+rwX", str(pkgs)], check=False)


def main() -> None:
    repo_root = Path(__file__).resolve().parents[3]
    dbt_dir = repo_root / "dbt"

    env = dict(os.environ)
    env["PYTHONUNBUFFERED"] = "1"

    uv = "uv"
    _run([uv, "run", "dbt", "deps", "--target", "prod"], cwd=dbt_dir, env=env)
    _run(
        [uv, "run", "dbt", "build", "--target", "prod", "--fail-fast"],
        cwd=dbt_dir,
        env=env,
    )
    _fix_dbt_packages_ownership(dbt_dir)


if __name__ == "__main__":
    main()
