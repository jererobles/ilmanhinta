"""Initialize DAGSTER_HOME with minimal production config if missing.

This seeds dagster.yaml and workspace.yaml under the directory pointed by
the DAGSTER_HOME environment variable (defaults to /app/data/dagster_home).
Intended to run at deploy (Fly release_command) before daemon starts.
"""

from __future__ import annotations

import os
from pathlib import Path
from textwrap import dedent

from loguru import logger


def ensure_file(path: Path, content: str) -> None:
    if path.exists():
        logger.info(f"DAGSTER_HOME file present: {path}")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")
    logger.info(f"Wrote {path}")


def main() -> None:
    dagster_home = Path(os.getenv("DAGSTER_HOME", "/app/data/dagster_home"))
    dagster_home.mkdir(parents=True, exist_ok=True)

    dagster_yaml = dagster_home / "dagster.yaml"
    workspace_yaml = dagster_home / "workspace.yaml"

    # Minimal, sqlite-backed instance with daemon scheduler and local logs.
    dagster_yaml_content = (
        dedent(
            f"""
        instance_class: dagster._core.instance.DagsterInstance

        scheduler:
          module: dagster._core.scheduler
          class: DagsterDaemonScheduler

        run_storage:
          module: dagster._core.storage.runs
          class: SqliteRunStorage
          config:
            base_dir: {dagster_home}

        event_log_storage:
          module: dagster._core.storage.event_log
          class: SqliteEventLogStorage
          config:
            base_dir: {dagster_home}

        schedule_storage:
          module: dagster._core.storage.schedules
          class: SqliteScheduleStorage
          config:
            base_dir: {dagster_home}

        compute_logs:
          module: dagster._core.storage.local_compute_log_manager
          class: LocalComputeLogManager
          config:
            base_dir: {dagster_home}/storage

        local_artifact_storage:
          module: dagster._core.storage.root
          class: LocalArtifactStorage
          config:
            base_dir: {dagster_home}/storage
        """
        ).strip()
        + "\n"
    )

    # Single in-process code location from python module
    workspace_yaml_content = (
        dedent(
            """
        load_from:
          - python_module: ilmanhinta.dagster
        """
        ).strip()
        + "\n"
    )

    ensure_file(dagster_yaml, dagster_yaml_content)
    ensure_file(workspace_yaml, workspace_yaml_content)


if __name__ == "__main__":
    main()
