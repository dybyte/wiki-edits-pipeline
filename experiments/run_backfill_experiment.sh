#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 || ! "$1" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
  echo "사용법: $0 YYYY-MM-DD" >&2
  exit 2
fi

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
day="$1"
next_day="$(.venv/bin/python -c 'import datetime as d, sys; print(d.date.fromisoformat(sys.argv[1]) + d.timedelta(days=1))' "$day")"

ch() {
  docker compose exec -T clickhouse clickhouse-client \
    --user wiki --password wiki --query "$1"
}

checksum() {
  ch "
    select concat(
      toString(count()), ':',
      toString(sum(cityHash64(toString(tuple(
        event_hour, wiki, edit_count, new_page_count, bot_ratio, bytes_delta, editor_count
      )))))
    )
    from marts.fct_edits_hourly
    where toDate(event_hour) = toDate('${day}')
    format TSV
  "
}

echo "[1/3] 기준 mart를 만듭니다."
(cd dbt && ../.venv/bin/dbt run --no-use-colors)
before="$(checksum)"
if [[ "$before" == 0:* ]]; then
  echo "${day}에 mart 행이 없습니다. 데이터가 있는 날짜를 고르세요." >&2
  exit 1
fi

echo "[2/3] ${day} 파티션을 같은 staging 원본으로 다시 계산합니다."
(cd dbt && ../.venv/bin/dbt run --no-use-colors --select fct_edits_hourly \
  --vars "{reprocess_from: '${day}', reprocess_until: '${next_day}'}")
after="$(checksum)"

echo "[3/3] 재처리 전후 체크섬을 비교합니다."
if [[ "$before" != "$after" ]]; then
  echo "FAIL: ${before} -> ${after}" >&2
  exit 1
fi
echo "PASS: ${day}의 행 수와 값 체크섬이 ${after}로 동일합니다."
