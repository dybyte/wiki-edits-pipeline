#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

ch() {
  docker compose exec -T clickhouse clickhouse-client \
    --user wiki --password wiki --query "$1"
}

cleanup() {
  ch "alter table raw.raw_recentchange delete where kafka_partition = -999 settings mutations_sync = 2" >/dev/null || true
}
trap cleanup EXIT
cleanup

echo "[1/4] 기준 mart와 테스트를 만듭니다."
(cd dbt && ../.venv/bin/dbt run --no-use-colors && ../.venv/bin/dbt test --no-use-colors)
before="$(ch "select sum(edit_count) from marts.fct_edits_hourly format TSV")"

echo "[2/4] 최근 위키백과 문서 편집 최대 1,000건을 raw에 한 번 더 넣습니다."
ch "
  insert into raw.raw_recentchange
    (event_id, event_ts, payload, kafka_partition, kafka_offset)
  select
    event_id, event_ts, payload, -999, kafka_offset
  from raw.raw_recentchange
  where JSONExtractString(payload, 'type') in ('edit', 'new')
    and JSONExtractString(payload, 'server_name') like '%.wikipedia.org'
    and JSONExtractInt(payload, 'namespace') = 0
  order by event_ts desc
  limit 1000
"
inserted="$(ch "select count() from raw.raw_recentchange where kafka_partition = -999 format TSV")"
if [[ "$inserted" == "0" ]]; then
  echo "실험할 원본 행이 없습니다. producer/consumer 또는 load_sample.py로 먼저 데이터를 넣으세요." >&2
  exit 1
fi
duplicates="$(ch "select sum(c - 1) from (select count() c from raw.raw_recentchange group by event_id having c > 1) format TSV")"
echo "  실험 중복 삽입: ${inserted}건, raw 전체 중복: ${duplicates}건"

echo "[3/4] staging dedup 뒤 dbt 모델과 테스트를 다시 실행합니다."
(cd dbt && ../.venv/bin/dbt run --no-use-colors && ../.venv/bin/dbt test --no-use-colors)
after="$(ch "select sum(edit_count) from marts.fct_edits_hourly format TSV")"

echo "[4/4] mart 값이 변하지 않았는지 확인합니다."
if [[ "$before" != "$after" ]]; then
  echo "FAIL: mart edit_count가 ${before}에서 ${after}로 바뀌었습니다." >&2
  exit 1
fi
echo "PASS: raw에는 중복이 있지만 mart edit_count는 ${after}로 동일합니다."
