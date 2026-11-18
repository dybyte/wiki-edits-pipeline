#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

echo "== Kafka raw-loader offsets and lag =="
docker compose exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --group raw-loader

echo
echo "== ClickHouse raw rows =="
docker compose exec -T clickhouse clickhouse-client \
  --user wiki \
  --password wiki \
  --query "
    select
      count() as raw_rows,
      uniqExact(event_id) as unique_events,
      count() - uniqExact(event_id) as duplicate_rows,
      max(ingested_at) as latest_ingested_at
    from raw.raw_recentchange
    format PrettyCompact
  "
