"""Kafka → ClickHouse(raw) 컨슈머.

wiki.recentchange 토픽을 묶음으로 읽어 raw.raw_recentchange에 넣고,
DW 적재가 끝난 뒤에만 Kafka 오프셋을 커밋한다.

    python ingest/consumer.py

흐름
  1. 토픽을 구독한다.
  2. 메시지를 받아 batch에 모은다.
  3. batch가 2,000건이 되거나 첫 건을 받은 지 1분이 지나면 flush 한다: raw에 insert하고 오프셋을 커밋한다.
  4. Ctrl+C나 SIGTERM을 받으면 남은 batch를 flush 하고 끝낸다.

결정 (2026-10-22, 이유는 계획서의 "설계 결정 노트" 탭)
  - 처음 시작 위치는 earliest: 컨슈머를 켜기 전에 Kafka에 쌓인 메시지도 다 읽는다.
  - 자동 커밋은 끈다: 1분 동안 모으는 사이에 자동 커밋이 돌면, DW에 없는 메시지가 처리 완료로 기록된다.
  - insert → commit 순서: 둘 사이에서 죽으면 중복이 생기고(at-least-once), 중복은 staging에서 event_id로 거른다.
"""
import datetime as dt
import json
import logging
import os
import signal
import time
from types import FrameType

import clickhouse_connect
from clickhouse_connect.driver.client import Client
from confluent_kafka import Consumer, Message

BOOTSTRAP = "localhost:9092"
TOPIC = "wiki.recentchange"
GROUP_ID = "raw-loader"
CLICKHOUSE = {"host": "localhost", "port": 8123, "username": "wiki", "password": "wiki"}
TABLE = "raw.raw_recentchange"
COLUMNS = ["event_id", "event_ts", "payload", "kafka_partition", "kafka_offset"]
FLUSH_MAX_ROWS = int(os.getenv("WIKI_FLUSH_MAX_ROWS", "2000"))
FLUSH_MAX_SEC = float(os.getenv("WIKI_FLUSH_MAX_SEC", "60"))
CONSUME_MAX_MESSAGES = int(os.getenv("WIKI_CONSUME_MAX_MESSAGES", "500"))
MAX_FLUSHES = int(os.getenv("WIKI_MAX_FLUSHES", "0"))   # 0이면 계속 실행, 양수면 실험용 종료 횟수
CRASH_AFTER_INSERT_ENV = "WIKI_CRASH_AFTER_INSERT"

log = logging.getLogger("consumer")


def to_row(msg: Message) -> list[object]:
    """Kafka 메시지 하나를 raw 테이블의 한 행으로 바꾼다. 값의 순서는 COLUMNS와 같다."""
    payload = msg.value().decode("utf-8")   # 프로듀서가 보낸 JSON 문자열 그대로
    event = json.loads(payload)
    return [
        event["meta"]["id"],
        dt.datetime.fromisoformat(event["meta"]["dt"]),
        payload,
        msg.partition(),
        msg.offset(),
    ]


def flush(client: Client, consumer: Consumer, batch: list[list[object]]) -> None:
    """모아 둔 batch를 DW에 넣고 Kafka 오프셋을 커밋한다."""
    if not batch:
        return
    # 순서가 핵심이다: DW에 먼저 넣고, 그다음에 Kafka 책갈피(오프셋)를 옮긴다.
    # 둘 사이에서 죽으면 같은 batch를 다시 읽어 또 넣으므로 중복이 생기지만, 유실은 없다.
    client.insert(TABLE, batch, column_names=COLUMNS)   # DW에 한 번에 넣기. 실패하면 예외가 나서 커밋까지 가지 않는다
    if os.getenv(CRASH_AFTER_INSERT_ENV) == "1":
        # 실험 1b 전용 fault injection. finally/flush를 거치지 않는 kill -9와 같은 종료를 만든다.
        # 이 환경 변수는 실험 실행 한 번에만 주고, 재시작할 때는 반드시 빼야 한다.
        log.error("장애 실험: ClickHouse insert 뒤 Kafka commit 전에 강제 종료합니다")
        os._exit(91)
    consumer.commit(asynchronous=False)                  # 지금까지 읽은 위치를 Kafka에 저장 (끝날 때까지 기다림)
    log.info("%s건 적재, 오프셋 커밋", f"{len(batch):,}")


def handle_sigterm(signum: int, frame: FrameType | None) -> None:
    # docker stop이나 kill(-9가 아닌)이 보내는 SIGTERM을 Ctrl+C와 같게 처리한다
    raise KeyboardInterrupt


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%H:%M:%S")
    signal.signal(signal.SIGTERM, handle_sigterm)

    consumer: Consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": GROUP_ID,
        # 커밋은 flush()에서 DW 적재가 끝난 뒤에만 직접 한다
        "enable.auto.commit": False,
        # 이 그룹이 커밋한 오프셋이 없을 때(처음 켤 때) 토픽에 남아 있는 가장 오래된 메시지부터 읽는다
        "auto.offset.reset": "earliest",
    })
    consumer.subscribe([TOPIC])
    client: Client = clickhouse_connect.get_client(**CLICKHOUSE)
    log.info("%s 구독 시작", TOPIC)

    batch: list[list[object]] = []   # 한 행 = to_row가 만든 리스트
    flushes = 0
    # 지금 batch에 첫 메시지가 들어온 시각. time.monotonic()은 경과 시간 측정용 시계다 (Java의 System.nanoTime(), 단위는 초)
    batch_started_at: float = time.monotonic()
    try:
        while True:
            # 최대 500건을, 최대 1초 기다려서 받는다. 아무것도 없으면 빈 리스트가 온다
            for msg in consumer.consume(num_messages=CONSUME_MAX_MESSAGES, timeout=1.0):
                if msg.error():
                    raise RuntimeError(msg.error())
                if not batch:
                    batch_started_at = time.monotonic()
                batch.append(to_row(msg))

            # 2,000건이 모였거나, 첫 건을 받은 지 1분이 지났으면 넣는다. 빈 batch는 flush가 알아서 건너뛴다
            should_flush = (
                len(batch) >= FLUSH_MAX_ROWS
                or time.monotonic() - batch_started_at >= FLUSH_MAX_SEC
            )
            if should_flush:
                flushed_rows = len(batch)
                flush(client, consumer, batch)
                batch = []
                if flushed_rows:
                    flushes += 1
                if MAX_FLUSHES and flushes >= MAX_FLUSHES:
                    log.info("실험 설정에 따라 %d회 flush 후 종료합니다", flushes)
                    break
    except KeyboardInterrupt:
        log.info("종료 신호를 받았습니다. 남은 batch를 넣고 끝냅니다")
        flush(client, consumer, batch)
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
