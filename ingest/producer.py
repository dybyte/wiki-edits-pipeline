"""EventStreams(SSE) → Kafka 프로듀서.

Wikimedia의 recentchange 스트림을 읽어 Kafka 토픽 wiki.recentchange에 보낸다.
끊기거나 죽었다 다시 켜도, Kafka 전송이 확인된 마지막 지점부터 이어 받는다.

    python ingest/producer.py

흐름
  1. 체크포인트 파일에서 마지막 SSE id를 읽는다. 없으면 지금부터 받는다.
  2. EventStreams에 연결한다. id가 있으면 Last-Event-ID 헤더로 보내 그다음부터 받는다.
  3. 이벤트를 하나 받을 때마다 Kafka에 보낸다.
  4. 몇 초마다 보낸 메시지가 Kafka에 모두 도착했는지 확인(flush)하고, 그때까지 보낸 마지막 id를 저장한다.
  5. 연결이 끊기면 잠시 쉬었다가 2번부터 다시 한다.
  6. Ctrl+C나 SIGTERM을 받으면 남은 메시지를 다 보내고 위치를 저장한 뒤 끝낸다.
     kill -9는 막을 수 없어서, 마지막 저장 이후에 보낸 이벤트는 재시작 때 한 번 더 보낸다 (중복은 staging에서 거른다).
"""
import json
import logging
import os
import signal
import time
from collections.abc import Callable, Iterator
from pathlib import Path
from types import FrameType

import requests
from confluent_kafka import KafkaError, Message, Producer

# on_delivery 콜백의 타입: (오류 또는 None, 메시지)를 받고 아무것도 돌려주지 않는 함수
DeliveryCallback = Callable[[KafkaError | None, Message], None]

BOOTSTRAP = "localhost:9092"
TOPIC = "wiki.recentchange"
STREAM_URL = "https://stream.wikimedia.org/v2/stream/recentchange"
# Wikimedia 정책에 맞춰 프로젝트와 연락 가능한 공개 계정을 보낸다.
USER_AGENT = "wiki-edits-pipeline/0.1 (https://github.com/dybyte)"

# 프로젝트 폴더/.checkpoint/producer_last_event_id (어느 폴더에서 실행해도 같은 파일을 쓴다)
CHECKPOINT_FILE = Path(__file__).resolve().parent.parent / ".checkpoint" / "producer_last_event_id"
CHECKPOINT_EVERY_SEC = 5   # 이 간격마다 전송 완료를 확인하고 위치를 저장한다
LOG_EVERY = 1000           # 이 건수마다 진행 상황을 출력한다

log = logging.getLogger("producer")


def load_checkpoint() -> str | None:
    """저장해 둔 마지막 SSE id를 읽는다. 처음 실행이면 None."""
    if CHECKPOINT_FILE.exists():
        return CHECKPOINT_FILE.read_text().strip() or None
    return None


def save_checkpoint(sse_id: str) -> None:
    """SSE id를 파일에 저장한다.

    임시 파일에 다 쓴 뒤 이름을 바꿔 덮어쓴다. 이름 바꾸기는 한 번에 일어나서,
    쓰는 도중에 kill -9를 당해도 반쯤 쓰인 파일이 남지 않는다.
    """
    CHECKPOINT_FILE.parent.mkdir(parents=True, exist_ok=True)
    tmp = CHECKPOINT_FILE.with_suffix(".tmp")
    tmp.write_text(sse_id)
    os.replace(tmp, CHECKPOINT_FILE)


def read_stream(last_event_id: str | None) -> Iterator[tuple[str | None, str]]:
    """EventStreams에 연결해 (SSE id, data 문자열)을 하나씩 내보낸다.

    SSE 이벤트 하나는 여러 줄(id:, data: 등)과 빈 줄 하나로 이루어진다.
    줄을 모아 두다가 빈 줄을 만나면 그때 이벤트 하나를 내보낸다.
    반환 타입 Iterator[...]는 yield로 하나씩 내보낸다는 뜻이다 (Java의 Iterator<Pair<String, String>>).
    """
    headers: dict[str, str] = {"User-Agent": USER_AGENT}
    if last_event_id:
        headers["Last-Event-ID"] = last_event_id   # 서버가 이 위치 다음부터 보내 준다

    with requests.get(STREAM_URL, headers=headers, stream=True, timeout=30) as resp:
        resp.raise_for_status()
        sse_id: str | None = None
        data: str | None = None
        for line in resp.iter_lines(decode_unicode=True):
            if line == "":                       # 빈 줄 = 이벤트 하나 끝
                if data is not None:
                    yield sse_id, data
                sse_id, data = None, None
            elif line.startswith(":"):           # ':ok' 같은 주석 줄
                continue
            elif line.startswith("id: "):
                sse_id = line[len("id: "):]
            elif line.startswith("data: "):
                data = line[len("data: "):]
            # 'event: message' 줄은 쓰지 않는다


def send(producer: Producer, key: str, value: str, on_delivery: DeliveryCallback) -> None:
    """메시지 하나를 Kafka로 보낸다. 정확히는 프로듀서의 내부 대기열에 넣는다."""
    while True:
        try:
            producer.produce(TOPIC, key=key, value=value, on_delivery=on_delivery)
            break
        except BufferError:
            # 대기열이 꽉 찼다 (재생 중처럼 받는 속도가 보내는 속도보다 빠를 때). 조금 빠질 때까지 기다린다
            producer.poll(1)
    producer.poll(0)   # 도착 확인이 온 메시지의 on_delivery를 처리한다 (기다리지 않음)


def checkpoint(producer: Producer, sse_id: str | None, failed: list[KafkaError]) -> None:
    """지금까지 보낸 메시지가 Kafka에 모두 도착했는지 확인하고, 그랬을 때만 위치를 저장한다."""
    remaining = producer.flush(30)   # 대기열이 빌 때까지 최대 30초 기다린다
    if remaining > 0 or failed:
        # 위치를 저장하지 않고 멈춘다. 다시 켜면 마지막으로 저장한 위치부터 다시 보내므로 빠지는 이벤트가 없다
        raise RuntimeError(f"Kafka 전송 실패 (남은 메시지 {remaining}건, 오류 {failed[:1]})")
    if sse_id:
        save_checkpoint(sse_id)


def handle_sigterm(signum: int, frame: FrameType | None) -> None:
    # docker stop이나 kill(-9가 아닌)이 보내는 SIGTERM을 Ctrl+C와 같게 처리한다
    raise KeyboardInterrupt


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%H:%M:%S")
    signal.signal(signal.SIGTERM, handle_sigterm)

    producer: Producer = Producer({
        "bootstrap.servers": BOOTSTRAP,
        # 프로듀서가 내부에서 재전송할 때 같은 메시지가 Kafka에 두 번 쓰이지 않게 한다 (acks=all도 같이 켜진다)
        "enable.idempotence": True,
        "linger.ms": 50,            # 최대 50ms 동안 모아서 한 번에 보낸다
        "compression.type": "lz4",
    })

    failed: list[KafkaError] = []   # 전송에 실패한 메시지의 오류. 하나라도 있으면 위치를 저장하지 않는다

    def on_delivery(err: KafkaError | None, msg: Message) -> None:
        """Kafka가 메시지를 받았는지(err가 None) 실패했는지 알려 줄 때 불린다."""
        if err is not None:
            failed.append(err)

    last_sent_id: str | None = load_checkpoint()
    log.info("저장된 위치부터 이어 받습니다" if last_sent_id else "저장된 위치가 없어서 지금부터 받습니다")

    sent: int = 0
    last_checkpoint_at: float = time.monotonic()   # 경과 시간 측정용 시계 (Java의 System.nanoTime(), 단위는 초)
    retry_delay: int = 1
    try:
        while True:   # 연결이 끊기면 다시 연결한다
            try:
                for sse_id, data in read_stream(last_sent_id):
                    # key = 이벤트 고유 ID. 순서가 필요 없어서 파티션에 고르게 퍼지는 값을 골랐다
                    # (wiki로 하면 enwiki·commons가 있는 파티션에 쏠린다)
                    event_id = json.loads(data)["meta"]["id"]
                    send(producer, event_id, data, on_delivery)   # data는 받은 문자열 그대로 보낸다
                    if sse_id:
                        last_sent_id = sse_id   # 보낸 다음에 위치를 갱신한다. 순서가 반대면 죽었을 때 하나를 건너뛸 수 있다
                    sent += 1
                    retry_delay = 1

                    if sent % LOG_EVERY == 0:
                        log.info("%s건 보냄", f"{sent:,}")
                    if time.monotonic() - last_checkpoint_at >= CHECKPOINT_EVERY_SEC:
                        checkpoint(producer, last_sent_id, failed)
                        last_checkpoint_at = time.monotonic()
                # 스트림은 원래 끝나지 않지만, 서버 쪽이 연결을 정상적으로 닫을 수는 있다 (서버 재시작, 연결 시간 제한 등).
                # 그러면 오류 없이 for 루프가 끝나서 여기로 온다. 로그만 남기고 위의 while로 돌아가 다시 연결한다
                log.warning("스트림이 끝났습니다. 다시 연결합니다")
            except requests.RequestException as e:
                log.warning("스트림 연결 오류: %s. %d초 뒤 다시 연결합니다", e, retry_delay)
                time.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, 60)
    except KeyboardInterrupt:
        log.info("종료 신호를 받았습니다. 보내던 메시지를 마무리합니다")
        checkpoint(producer, last_sent_id, failed)
        log.info("종료: 이번 실행에서 %s건 보냄", f"{sent:,}")


if __name__ == "__main__":
    main()
