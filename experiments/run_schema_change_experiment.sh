#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

ch() {
  docker compose exec -T clickhouse clickhouse-client \
    --user wiki --password wiki --query "$1"
}

cleanup() {
  ch "alter table raw.raw_recentchange delete where kafka_partition = -998 settings mutations_sync = 2" >/dev/null || true
}
trap cleanup EXIT
cleanup

if [[ "$(ch "select count() from raw.raw_recentchange format TSV")" == "0" ]]; then
  echo "실험할 raw 데이터가 없습니다. producer/consumer 또는 load_sample.py로 먼저 데이터를 넣으세요." >&2
  exit 1
fi

event_id="experiment-schema-change-$(date +%s)"
echo "[1/3] staging이 모르는 future_field가 포함된 이벤트를 raw의 최신 날짜에 넣습니다."
ch "
  insert into raw.raw_recentchange
    (event_id, event_ts, payload, kafka_partition, kafka_offset)
  select
    '${event_id}',
    max(event_ts),
    '{\"\$schema\":\"/mediawiki/recentchange/1.0.0\",\"meta\":{\"id\":\"${event_id}\",\"dt\":\"2026-11-18T00:00:00Z\",\"domain\":\"example\"},\"wiki\":\"enwiki\",\"server_name\":\"en.wikipedia.org\",\"type\":\"edit\",\"namespace\":0,\"title\":\"Schema experiment\",\"user\":\"experiment\",\"bot\":false,\"minor\":false,\"length\":{\"old\":1,\"new\":2},\"future_field\":{\"nested\":true}}',
    -998,
    0
  from raw.raw_recentchange
"

echo "[2/3] 전체 모델을 갱신하고 데이터 품질 테스트를 실행합니다."
(cd dbt && ../.venv/bin/dbt run --no-use-colors \
  && ../.venv/bin/dbt test --no-use-colors)

echo "[3/3] 새 필드를 무시하면서 기존 필드를 정상 추출했는지 확인합니다."
extracted="$(ch "
  select concat(wiki, ':', event_type, ':', toString(byte_delta))
  from staging.stg_recentchange
  where event_id = '${event_id}'
  format TSV
")"
if [[ "$extracted" != "enwiki:edit:1" ]]; then
  echo "FAIL: 예상 enwiki:edit:1, 실제 ${extracted}" >&2
  exit 1
fi

cleanup
trap - EXIT
(cd dbt && ../.venv/bin/dbt run --no-use-colors >/dev/null)
echo "PASS: 알 수 없는 future_field가 있어도 ${extracted}로 처리했고 실험 데이터를 정리했습니다."
