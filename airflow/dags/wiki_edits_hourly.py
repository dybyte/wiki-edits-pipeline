"""매시간 wiki edits mart를 갱신하고 데이터 품질 테스트를 실행한다."""

from __future__ import annotations

import subprocess
from datetime import datetime, timedelta, timezone

from airflow.sdk import dag, task

DBT = "/opt/dbt-venv/bin/dbt"
PROJECT_DIR = "/opt/wiki-edits/dbt"
COMMON_ARGS = [
    "--project-dir",
    PROJECT_DIR,
    "--profiles-dir",
    PROJECT_DIR,
    "--target-path",
    "/tmp/dbt-target",
    "--log-path",
    "/tmp/dbt-logs",
    "--no-use-colors",
]


def run_dbt(command: str) -> None:
    """dbt 하위 명령을 실행하고 실패를 Airflow task 실패로 전달한다."""
    subprocess.run([DBT, command, *COMMON_ARGS], check=True)


@dag(
    dag_id="wiki_edits_hourly",
    schedule="@hourly",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["wiki-edits", "dbt"],
)
def wiki_edits_hourly():
    @task
    def build_models() -> None:
        run_dbt("run")

    @task
    def test_data() -> None:
        run_dbt("test")

    build_models() >> test_data()


wiki_edits_hourly()
