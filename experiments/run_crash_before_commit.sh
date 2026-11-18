#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

echo "producer가 실행 중이고 다른 raw-loader consumer는 꺼져 있어야 합니다."
echo "[1/4] 100건 insert 직후, offset commit 전에 consumer를 강제 종료합니다."
set +e
WIKI_FLUSH_MAX_ROWS=100 \
WIKI_FLUSH_MAX_SEC=10 \
WIKI_CONSUME_MAX_MESSAGES=100 \
WIKI_CRASH_AFTER_INSERT=1 \
  .venv/bin/python ingest/consumer.py
status=$?
set -e
if [[ "$status" != "91" ]]; then
  echo "예상한 fault-injection 종료 코드 91 대신 ${status}를 받았습니다." >&2
  exit 1
fi

echo "[2/4] 같은 미커밋 메시지를 다시 읽어 적재하고 한 번 flush한 뒤 정상 종료합니다."
WIKI_FLUSH_MAX_ROWS=100 \
WIKI_FLUSH_MAX_SEC=10 \
WIKI_CONSUME_MAX_MESSAGES=100 \
WIKI_MAX_FLUSHES=1 \
  .venv/bin/python ingest/consumer.py

echo "[3/4] raw 중복 수를 확인합니다."
docker compose exec -T clickhouse clickhouse-client \
  --user wiki --password wiki \
  --query "
    select count() - uniqExact(event_id) as raw_duplicate_rows
    from raw.raw_recentchange
    format PrettyCompact
  "

echo "[4/4] staging 중복 제거와 mart 테스트를 검증합니다."
(cd dbt && ../.venv/bin/dbt run --no-use-colors && ../.venv/bin/dbt test --no-use-colors)
echo "PASS: commit 전 장애 재현과 재처리 후 dbt 테스트가 통과했습니다."
