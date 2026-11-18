"""학습용 샘플 적재: 최근 N시간의 이벤트를 EventStreams에서 받아 raw 테이블에 넣는다.

Kafka를 거치지 않는 지름길이다. 컨슈머를 만들기 전에 dbt를 먼저 연습하려고 쓴다.
실행할 때마다 raw 테이블을 비우고 다시 채우며, 같은 event_id는 한 번만 넣는다.
컨슈머가 만드는 진짜 중복을 어떻게 다룰지는 직접 정한다 (GUIDE.md 참고).

    python ingest/load_sample.py --hours 2
"""
import argparse
import datetime as dt
import json
from collections.abc import Iterator

import clickhouse_connect
import requests

STREAM_URL = "https://stream.wikimedia.org/v2/stream/recentchange"
# Wikimedia 정책에 맞춰 프로젝트와 연락 가능한 공개 계정을 보낸다.
USER_AGENT = "wiki-edits-pipeline/0.1 (https://github.com/dybyte)"
CLICKHOUSE = {"host": "localhost", "port": 8123, "username": "wiki", "password": "wiki"}
COLUMNS = ["event_id", "event_ts", "payload"]

# ClickHouse는 insert 한 번마다 디스크에 조각(part)을 하나 만든다. 작은 insert를 자주 하면 조각이 너무 많아져
# 느려지거나 "Too many parts" 오류가 나므로, 모아서 크게 넣는다.
BATCH_SIZE = 20_000

# until 이후 이벤트가 이만큼 연속으로 오면 실시간까지 따라잡은 것으로 본다.
# 바로 멈추지 않는 이유: 데이터센터 토픽 두 개가 섞여 와서, 재생 초반에도 최신 시각 이벤트가 끼어 있다.
CAUGHT_UP_AFTER = 2000


def read_events(since: dt.datetime) -> Iterator[dict]:
    """SSE 스트림에서 이벤트(dict)를 하나씩 꺼낸다."""
    params = {"since": since.strftime("%Y-%m-%dT%H:%M:%SZ")}
    headers = {"User-Agent": USER_AGENT}
    with requests.get(STREAM_URL, params=params, headers=headers, stream=True, timeout=30) as resp:
        resp.raise_for_status()
        for line in resp.iter_lines(decode_unicode=True):
            # SSE는 'event:', 'id:', 'data:' 줄과 빈 줄로 이벤트 하나를 보낸다.
            # 이벤트 내용은 'data:' 줄에 JSON으로 들어 있다.
            if line and line.startswith("data: "):
                yield json.loads(line[len("data: "):])


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--hours", type=float, default=2, help="몇 시간 전부터 받을지")
    args = parser.parse_args()

    until = dt.datetime.now(dt.timezone.utc).replace(microsecond=0)
    since = until - dt.timedelta(hours=args.hours)
    print(f"{since:%m-%d %H:%M} ~ {until:%H:%M} UTC 구간을 받습니다.")

    client = clickhouse_connect.get_client(**CLICKHOUSE)
    client.command("truncate table raw.raw_recentchange")

    seen: set[str] = set()
    batch: list[list[object]] = []
    rows = duplicates = past_until = 0   # 셋 다 int
    for event in read_events(since):
        event_ts = dt.datetime.fromisoformat(event["meta"]["dt"])
        if event_ts >= until:
            past_until += 1
            if past_until >= CAUGHT_UP_AFTER:
                break
            continue

        event_id = event["meta"]["id"]
        if event_id in seen:
            duplicates += 1
            continue
        seen.add(event_id)

        batch.append([event_id, event_ts, json.dumps(event, ensure_ascii=False)])
        if len(batch) >= BATCH_SIZE:
            client.insert("raw.raw_recentchange", batch, column_names=COLUMNS)
            rows += len(batch)
            batch = []
            if rows % 100_000 == 0:
                print(f"  {rows:,}건 넣음 ({event_ts:%H:%M} UTC까지)")

    if batch:
        client.insert("raw.raw_recentchange", batch, column_names=COLUMNS)
        rows += len(batch)

    print(f"완료: raw.raw_recentchange에 {rows:,}건을 넣었습니다. (같은 event_id 중복 {duplicates}건 건너뜀)")


if __name__ == "__main__":
    main()
