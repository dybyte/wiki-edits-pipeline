# wiki-edits-pipeline

위키백과 실시간 편집 스트림(Wikimedia EventStreams)을 Kafka로 수집해 ClickHouse에 적재하고,
dbt로 시간별·위키별 편집 지표를 만드는 로컬 데이터 파이프라인이다. Airflow가 매시간 변환과 테스트를 실행하고,
Prometheus·Grafana로 Kafka 유입량과 컨슈머 lag을 관찰한다.

컨슈머는 ClickHouse 적재 뒤 Kafka offset을 commit하는 at-least-once 구조다. 그 사이에 죽어 생긴 raw 중복은
staging에서 제거하며, 날짜 파티션 재처리와 장애 실험으로 최종 mart의 정합성을 검증한다.

## 실행

```bash
docker compose up -d
uv venv --python 3.12 .venv
uv pip install --python .venv/bin/python -r requirements.txt
.venv/bin/python ingest/load_sample.py --hours 2
cd dbt && ../.venv/bin/dbt run --full-refresh && ../.venv/bin/dbt test
```

`--full-refresh`는 hourly mart를 날짜 파티션 incremental 테이블로 처음 만들 때만 필요하다.
그 뒤의 수동 실행과 Airflow 예약 실행은 일반 `dbt run`으로 최근 파티션만 교체한다.

실시간 경로는 별도 터미널에서 실행한다.

```bash
.venv/bin/python ingest/producer.py
.venv/bin/python ingest/consumer.py
```

- Kafka 지표와 consumer lag: Grafana http://localhost:3000
- 매시간 dbt `run → test`: Airflow http://localhost:8080
- Airflow 최초 admin 비밀번호: `docker compose logs airflow | grep -i password`

Airflow를 포함한 이미지를 처음 만들 때는 `docker compose up -d --build`를 사용한다.
DAG는 처음부터 활성화되어 컨테이너 시작 후 해당 시간의 실행을 한 번 시작한다.

## 로컬 보안과 영속 데이터

Compose의 공개 포트는 모두 `127.0.0.1`에만 바인딩한다. 따라서 이 Mac에서는
`localhost:3000`처럼 접속할 수 있지만, 같은 Wi-Fi나 LAN의 다른 기기는 해당 포트에 접근할 수 없다.
컨테이너끼리는 공개 포트가 아니라 Docker 내부 네트워크의 서비스 이름(`clickhouse`, `kafka`)으로 통신하므로
이 제한을 적용해도 파이프라인 내부 연결은 달라지지 않는다.

`airflowdata`는 Docker named volume이다. `./airflowdata` 같은 프로젝트 폴더가 아니라 Docker가 별도로 관리하며,
Airflow의 SQLite metadata DB, 로그, 자동 생성한 로컬 관리자 비밀번호 파일을 `/opt/airflow`에 보존한다.
그래서 `docker compose stop`이나 컨테이너 재생성 뒤에도 상태가 남고 Git에는 포함되지 않는다.
다만 암호화된 비밀 저장소는 아니므로 로컬 개발용으로만 사용한다. `docker compose down -v`는 이 volume까지 삭제하므로
Airflow 실행 기록과 관리자 비밀번호도 함께 사라진다.

## 정합성 전략

1. consumer는 batch를 ClickHouse에 넣은 뒤에만 Kafka offset을 commit한다.
2. insert와 commit 사이의 장애는 유실 대신 raw 중복을 만든다.
3. `stg_recentchange`는 `event_id`별 가장 최근 적재 한 건만 남긴다.
4. `fct_edits_hourly`는 날짜 파티션을 `insert_overwrite`하며 평소 최근 이틀을 다시 계산한다.
5. 명시적 백필은 `--vars '{reprocess_from: YYYY-MM-DD, reprocess_until: YYYY-MM-DD}'`로 같은 날짜를 안전하게 교체한다.

재현 가능한 장애·백필 실험은 [experiments/README.md](experiments/README.md)에 있다.

## 테스트

```bash
.venv/bin/python -m unittest discover -s tests -v
cd dbt && ../.venv/bin/dbt test
```

## 구조

```
wiki-edits-pipeline/
├── docker-compose.yml   # ClickHouse(DW), Kafka, kafka-exporter, Prometheus, Grafana
├── monitoring/          # Prometheus 수집 설정, Grafana 데이터 소스·대시보드
├── warehouse/init/      # raw 테이블 DDL
├── ingest/              # 프로듀서, 컨슈머, 학습용 샘플 적재
├── dbt/                 # staging, marts, tests
├── airflow/             # 매시간 dbt run → test를 실행하는 로컬 Airflow
└── experiments/         # 장애·중복·백필·스키마 변경 재현 스크립트와 결과
```
