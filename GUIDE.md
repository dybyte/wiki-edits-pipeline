# 학습 가이드

이 레포가 지금 어떤 상태인지, 파일마다 무슨 역할을 하는지, 1주 차에 무엇을 직접 만드는지 정리한 문서다.
README는 나중에 면접관이 읽을 문서로 따로 쓴다.

## 0. 지금 상태

| 부분 | 상태 | 누가 만드나 |
|---|---|---|
| `docker-compose.yml` (ClickHouse, Kafka) | 동작함 | 제공 |
| `warehouse/init/01_raw.sql` (raw 테이블) | 동작함 | 제공 |
| `ingest/load_sample.py` (학습용 지름길) | 동작함 | 제공 |
| `ingest/producer.py` | 동작함 (2026-10-22 유실·중복 확인) | 제공 (요청에 따라 Claude가 작성). 한 줄씩 설명할 수 있어야 한다 |
| `ingest/consumer.py` | 완성 | insert→commit, 종료 처리, commit 전 장애 주입과 단위 테스트 포함 |
| `dbt/` staging, marts, tests | 완성 | staging 중복 제거, hourly 날짜 파티션 incremental, mart 대조 테스트 포함 |
| `airflow/dags/` | 완성 | Airflow 3 로컬 standalone에서 매시간 `dbt run` → `dbt test` |
| `experiments/` | 실행 도구 완성 | lag·중복·commit 전 장애·백필·스키마 변경 스크립트. 실제 숫자는 실행 후 기록 |
| `monitoring/` (kafka-exporter, Prometheus, Grafana) | 동작함 (2026-10-29 임시 토픽으로 lag 상승·해소 확인) | 제공. 사용법은 10장 |

나눈 기준은 계획서의 "내 것으로 만드는 작업 규칙"이다. 컨슈머의 적재·커밋 순서, 중복 흡수(upsert에 해당하는 부분),
dbt incremental 모델은 직접 짜고, Docker 설정과 보일러플레이트는 AI에 맡겨도 된다.

## 1. 데이터가 흐르는 길

```
[1주 차: 수집]                                                  [2주 차: 변환]
EventStreams → producer.py → Kafka → consumer.py → raw  ──dbt run──► staging ─► marts ──dbt test──► 통과 / 실패
                 (직접)                 (직접)        ▲
                                                     │
                                       load_sample.py (컨슈머가 생기기 전까지 쓰는 지름길)
```

ClickHouse 안에는 데이터베이스가 세 개 있다. (dbt의 "schema"가 ClickHouse에서는 "database"다.)

| 데이터베이스 | 테이블 | 누가 채우나 | 한 행 = |
|---|---|---|---|
| `raw` | `raw_recentchange` | 컨슈머 (지금은 `load_sample.py`) | 받은 이벤트 하나, JSON 원본 그대로. **중복도 그대로** |
| `staging` | `stg_recentchange` (view) | dbt | 이벤트 하나, 컬럼으로 펼치고 정리한 것 |
| `marts` | `fct_edits_hourly`, `fct_edits_daily` | dbt | (시간, 위키) / (날짜, 위키) |

## 2. 이미 아는 것과 연결하기

| 이 프로젝트에서 | 이미 해 본 것 | 새로 배우는 것 |
|---|---|---|
| 컨슈머: DW 적재가 끝난 뒤 오프셋 커밋 | SeaTunnel BigQuery 싱크의 체크포인트와 2단계 커밋 | 훨씬 단순한 버전. 트랜잭션 대신 "적재 후 커밋, 중복은 나중에 흡수" |
| raw는 중복을 받고, staging에서 `event_id`마다 한 줄만 남기기 | BigQuery 싱크에서 "이미 기록된 데이터는 성공으로 처리" | 같은 생각을 쓸 때가 아니라 **읽을 때** SQL로 구현하기 |
| raw에 JSON 원본, staging에서 필요한 컬럼만 꺼내기 | PostgreSQL CDC 스키마 변경 처리 | 스키마 변경을 저장할 때가 아니라 읽을 때 다룬다 |
| dbt test | 문제를 테스트로 재현하고 남겨 두는 작업 방식 | 검사 대상이 코드가 아니라 데이터 |
| raw의 시각 컬럼을 `DateTime64(3, 'UTC')`로 고정 | Flowing의 시간대 불일치 문제 | 집계 기준 시각을 서버 설정에 맡기지 않는다 |
| Kafka 컨슈머 설정 | Kafka 컨슈머 내부 동작 (close, 리밸런스) | Python 클라이언트(confluent-kafka) 사용법 |

정말 처음 배우는 것은 Python, dbt, Airflow, ClickHouse, 그리고 마트 설계(fact 테이블, 비가산 지표)다.

## 3. 지금 돌려 보기

```bash
docker compose up -d
uv venv --python 3.12 .venv
uv pip install --python .venv/bin/python -r requirements.txt
.venv/bin/python ingest/load_sample.py --hours 2      # 최근 2시간치를 raw에 넣는다 (1~2분)
cd dbt
../.venv/bin/dbt run                                  # staging, marts를 만든다
../.venv/bin/dbt test                                 # 테스트 5개
```

ClickHouse 안을 직접 보려면:

```bash
docker compose exec clickhouse clickhouse-client --user wiki --password wiki
```

`show databases`, `show tables from marts`, `select * from marts.fct_edits_hourly where wiki = 'kowiki'` 같은 쿼리를 쳐 볼 수 있다.
다 쓰고 나면 `docker compose stop`으로 끈다.

### 2026-10-20 실행 결과 (13:40~15:40 UTC)

| 단계 | 행 수 |
|---|---|
| raw | 224,530 |
| staging (canary 4건 제외) | 224,526 |
| 그중 위키백과 일반 문서 편집 (`is_article_edit`) | 29,879 (13%) |
| `fct_edits_hourly` | 346 |
| `fct_edits_daily` | 152 |

raw의 JSON 원본은 약 298MB인데, ClickHouse 디스크에는 압축돼서 약 79MB로 저장됐다. 열 지향 저장소는 같은 컬럼의 값을 모아 저장해서 압축이 잘 된다.

한국어 위키백과(`kowiki`)의 시간별 마트:

| event_hour (UTC) | edit_count | new_page_count | bot_ratio | bytes_delta | editor_count |
|---|---|---|---|---|---|
| 13:00 | 38 | 4 | 0.053 | 104,499 | 16 |
| 14:00 | 132 | 14 | 0.129 | 92,394 | 31 |
| 15:00 | 64 | 4 | 0.094 | -37,912 | 18 |

13시 행은 13:40~14:00만, 15시 행은 15:00~15:40만 들어 있어서 숫자가 작다. 15시의 `bytes_delta`가 음수인 건 그 시간에 지운 양이 추가한 양보다 많았다는 뜻이다.

## 4. dbt 파일 읽는 법

- `{{ source('raw', 'raw_recentchange') }}`: dbt가 만들지 않은 테이블(raw)을 가리킨다. `models/staging/_sources.yml`에 등록돼 있다.
- `{{ ref('stg_recentchange') }}`: dbt가 만든 다른 모델을 가리킨다. dbt는 `ref`를 보고 실행 순서(staging → hourly → daily)를 정한다.
- `materialized`: 테이블을 어떻게 만들지 정한다. `dbt_project.yml`에서 staging은 `view`, marts는 `table`로 정해 두었다.
- 파일 이름이 테이블 이름이 되고, `select`의 별칭(`as edit_count`)이 컬럼 이름이 된다.

dbt가 실제로 DB에 보낸 SQL은 `dbt/target/run/` 아래와 `dbt/logs/dbt.log`에 남는다. marts의 `table` 모델은 이렇게 만들어진다.

```sql
-- 1) 새 테이블(__dbt_backup)을 만들고 select 결과를 넣는다
insert into `marts`.`fct_edits_hourly__dbt_backup` (...)
select toStartOfHour(event_ts) as event_hour, wiki, count() as edit_count, ...
from `staging`.`stg_recentchange`     -- ref()가 실제 테이블 이름으로 바뀌었다
where is_article_edit
group by event_hour, wiki;

-- 2) 기존 테이블과 통째로 맞바꾼다
EXCHANGE TABLES `marts`.`fct_edits_hourly__dbt_backup` AND `marts`.`fct_edits_hourly`;
```

다 만든 뒤에 맞바꾸기 때문에, 다시 만드는 도중에도 기존 테이블을 조회할 수 있다.

테스트도 같은 방식이다. `_staging.yml`의 `unique` 한 단어는
`dbt/target/compiled/wiki_edits/models/staging/_staging.yml/unique_stg_recentchange_event_id.sql`에서 이렇게 바뀌어 있다.

```sql
select event_id as unique_field, count(*) as n_records
from `staging`.`stg_recentchange`
where event_id is not null
group by event_id
having count(*) > 1        -- 결과가 한 행이라도 나오면 FAIL
```

## 5. 테스트가 틀린 숫자를 잡는 장면 보기

`fct_edits_daily.sql`에는 일부러 틀린 방법(시간별 값을 더하기)이 같이 들어 있다. 변수 하나로 바꿔 끼울 수 있다.

```bash
cd dbt
../.venv/bin/dbt run --vars '{daily_from_hourly: true}' --select fct_edits_daily   # 틀린 방법으로 만들기
../.venv/bin/dbt test --select assert_daily_matches_staging                         # FAIL 103
../.venv/bin/dbt run --select fct_edits_daily                                       # 원래대로 되돌리기
```

2026-10-20에 실제로 돌렸을 때 테스트가 잡아낸 행 일부:

| wiki | 마트 edit_count | 기대값 | 마트 editor_count | 기대값 |
|---|---|---|---|---|
| enwiki | 8,611 | 8,611 | **3,132** | **2,531** |
| frwiki | 2,282 | 2,282 | **637** | **494** |
| dewiki | 1,945 | 1,945 | **645** | **517** |
| jawiki | 1,106 | 1,106 | **398** | **329** |
| kowiki | 234 | 234 | **65** | **49** |

편집 수는 더해도 맞지만, 편집자 수는 여러 시간에 걸쳐 편집한 사람이 시간마다 한 번씩 세어져서 부풀어 오른다.
이렇게 더하면 안 되는 값을 **비가산 지표(non-additive)**라고 한다.

이 테스트는 마트에서 행이 빠지거나 없는 행이 생겨도 잡는다. 마트에서 kowiki 행을 빼고 돌리면
`kowiki | 마트 0 | 기대 234`가, 가짜 행을 넣으면 `fakewiki | 마트 5 | 기대 0`이 나온다 (2026-10-20 확인).

## 6. 중복이 들어오면: 실험 1

raw 테이블은 중복을 막지 않는다. 컨슈머가 같은 이벤트를 두 번 넣는 상황을 흉내 내 보면 이렇다.

```bash
ch() { docker compose exec -T clickhouse clickhouse-client --user wiki --password wiki --query "$1"; }
ch "select now64(3)"     # 지금 시각을 적어 둔다 (나중에 지울 때 쓴다)
ch "insert into raw.raw_recentchange (event_id, event_ts, payload)
    select event_id, event_ts, payload from raw.raw_recentchange
    where JSONExtractString(payload,'type')='edit' and JSONExtractString(payload,'server_name') like '%.wikipedia.org'
      and JSONExtractInt(payload,'namespace')=0 limit 1000"
ch "select count(), uniqExact(event_id) from raw.raw_recentchange"
(cd dbt && ../.venv/bin/dbt run && ../.venv/bin/dbt test)
ch "delete from raw.raw_recentchange where ingested_at >= '<위에서 적어 둔 시각>'"   # 정리
```

2026-10-20에 실제로 돌린 결과:

| 확인한 것 | 결과 |
|---|---|
| raw 행 수 / 고유 event_id 수 | 225,530 / 224,530. 중복 1,000건이 그대로 들어갔다 |
| 마트의 전체 `edit_count` | 29,879 → **30,879**. 정확히 1,000만큼 부풀었다 |
| `unique_stg_recentchange_event_id` | **FAIL 1000** |
| `assert_daily_matches_staging` | PASS. 마트와 기대값이 같은 staging(중복 포함)을 읽어서 둘 다 똑같이 틀렸기 때문이다 |

여기서 두 가지를 알 수 있다.

1. **중복을 걸러야 하는 곳은 staging이다.** `stg_recentchange.sql`은 `event_id`별 최근 적재 한 건만 남긴다.
   `experiments/run_duplicate_experiment.sh`를 실행하면 raw에는 중복이 생겨도 mart 숫자는 그대로인지 자동으로 검사한다.
2. **테스트마다 잡는 문제가 다르다.** 대조 테스트는 "마트 계산이 맞는가"를 보고, `unique`는 "입력이 깨끗한가"를 본다. 하나만으로는 부족하다.

## 7. 수집과 중복 처리 재현

계획서 규칙 1번대로, 코드를 쓰기 전에 각 파일 맨 위의 질문에 노트로 먼저 답한다.

1. **`ingest/producer.py` 읽고 돌려 보기** (완성돼 있다): EventStreams를 읽어 Kafka 토픽 `wiki.recentchange`에 보낸다.
   5초마다 Kafka 전송이 끝났는지 확인하고 SSE 위치를 `.checkpoint/producer_last_event_id`에 저장한다.
   처음부터 다시 받고 싶으면 이 파일을 지운다.
   ```bash
   .venv/bin/python ingest/producer.py
   docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic wiki.recentchange --from-beginning --max-messages 3
   ```
   2026-10-22 확인: 정상 종료 → 재시작 → `kill -9` → 재시작으로 Kafka에 1,464건이 쌓였다.
   Wikimedia 쪽 오프셋이 빈틈없이 이어져 **유실 0**, 중복은 마지막 위치 저장부터 `kill -9`까지 약 2.5초 구간의 **67건**이었다.
2. **`ingest/consumer.py` 돌려 보기** (완성돼 있다): 시작 위치 `earliest`, 자동 커밋 끔, 2,000건 또는 1분마다 flush, insert → commit.
   결정과 이유는 계획서의 "설계 결정 노트" 탭에 있다.
   시작 전에 `load_sample.py`가 넣은 데이터를 비운다: `ch "truncate table raw.raw_recentchange"`.
   이후로는 `load_sample.py`를 돌리지 않는다 (시작하자마자 raw를 비운다).
   확인 방법: `ch "select count(), uniqExact(event_id) from raw.raw_recentchange"`로 행 수가 늘어나는지 보고,
   `docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group raw-loader`의 LAG가 0 근처인지 본다.
3. **중복 제거 검증**: `experiments/run_duplicate_experiment.sh`는 최근 편집을 raw에 복제한 뒤
   staging unique 테스트와 mart 총계 불변을 확인하고 실험 행을 정리한다.
4. **commit 전 장애 검증**: producer를 켜고 일반 consumer를 끈 상태에서
   `experiments/run_crash_before_commit.sh`를 실행한다. 100건 insert 뒤 프로세스가 종료되고,
   같은 미커밋 메시지를 재처리해 raw 중복과 staging 정합성을 함께 확인한다.

## 8. incremental과 Airflow

- `fct_edits_hourly`는 `insert_overwrite`로 날짜 파티션을 교체한다. 평소에는 target의 마지막 날짜와 전날을 다시 읽어
  늦게 도착한 이벤트를 흡수한다. 오래된 날짜는 `reprocess_from`, `reprocess_until` 변수로 명시한다.
- `experiments/run_backfill_experiment.sh YYYY-MM-DD`는 같은 날짜를 다시 계산한 전후의 행 수와 체크섬이 같은지 검사한다.
- Airflow DAG `wiki_edits_hourly`는 매시간 `dbt run` 뒤 `dbt test`를 실행한다. 로컬 UI는 http://localhost:8080 이다.
- Airflow는 학습용 `standalone` 구성이다. 실제 운영에서는 외부 metadata DB와 구성요소 분리가 필요하다.

## 9. 설계 결정 메모 (README로 옮길 것)

- **DW는 ClickHouse**: 열 지향 DW이고, Docker 컨테이너 하나로 로컬에서 돌아서 `docker compose up` 하나로 재현된다.
  BigQuery·Snowflake·Redshift처럼 primary key가 유일성을 보장하지 않으므로, 실제 DW에서처럼 중복을 읽을 때 걸러야 한다.
  처음에는 PostgreSQL로 만들었다가, PK 제약 조건이 중복을 대신 막아 줘서 실험 1의 의미가 사라진다는 점 때문에 바꿨다.
  DuckDB는 한 번에 한 프로세스만 쓸 수 있어서 컨슈머와 dbt가 동시에 쓸 수 없다.
- **raw는 그냥 `MergeTree`로 두고 중복을 받는다**: `ReplacingMergeTree`를 쓰면 같은 키의 행이 백그라운드 병합 때 합쳐지는데,
  병합 시점이 정해져 있지 않아서 실험 때 중복 수가 들쭉날쭉해진다. raw는 받은 그대로 두고 staging에서 확실하게 거른다.
- **raw는 날짜로 파티션을 나눈다** (`partition by toDate(event_ts)`): 백필 때 하루치만 바꿔 넣기 쉽다.
- **"편집"의 정의는 staging 한 곳에만 둔다** (`is_article_edit`): 위키백과(`*.wikipedia.org`)의 일반 문서(namespace 0)에 대한 `edit`과 `new`만 센다. 전체 이벤트 중 13%다.
- **집계 기준 시각은 `meta.dt`(발생 시각)이고, UTC로 고정한다.**
- **canary 이벤트는 staging에서 뺀다**: Wikimedia가 스트림 점검용으로 흘려보내는 이벤트로, 편집 정보가 없다.

### ClickHouse에서 걸린 점 (면접 소재)

- **JSON에서 값을 꺼낼 때 `Nullable`로 꺼내야 키가 없을 때 NULL이 된다.** 그냥 꺼내면 빈 문자열이나 0이 돼서 `not_null` 테스트가 canary를 못 잡는다.
- **`sum(edit_count) as edit_count`처럼 별칭과 컬럼 이름이 같으면 위험하다.** ClickHouse는 별칭을 식 안에서도 쓰게 해 줘서, 안쪽 `edit_count`가 별칭을 가리킬 수 있다. 테이블 이름을 붙여(`h.edit_count`) 피했다.
- **insert는 모아서 크게 한다.** insert 한 번마다 디스크 조각(part)이 하나 생겨서, 한 건씩 넣으면 "Too many parts" 오류가 난다.
- **FULL JOIN에서 짝이 없는 쪽 칸은 NULL이 아니라 기본값(0, 1970-01-01)으로 채워진다** (Nullable 컬럼은 NULL). 대조 테스트는 이 성질을 이용해 빠진 행을 잡는다.

## 10. 모니터링: 컨슈머를 멈추고 lag이 쌓이는 걸 눈으로 보기

`docker compose up -d` 하나로 아래 세 개가 같이 뜬다.

```
Kafka ◄──관리자 API── kafka-exporter ◄──10초마다 GET /metrics── Prometheus ◄──PromQL── Grafana
      (ListOffsets,    :9308                                    :9090                  :3000
       OffsetFetch)
```

| 이름 | 하는 일 | 주소 |
|---|---|---|
| kafka-exporter | Kafka에 파티션별 끝 오프셋과 그룹별 커밋 오프셋을 물어보고, 그 값을 Prometheus가 읽는 텍스트 형식으로 내놓는다 | http://localhost:9308/metrics |
| Prometheus | 등록된 주소에서 10초마다 값을 가져와(pull) 시각과 함께 저장하는 시계열 DB. PromQL로 조회한다 | http://localhost:9090 |
| Grafana | Prometheus에 쿼리를 보내 그래프로 그리는 화면. 저장소가 아니다 | http://localhost:3000 (로그인 없음) |

### 이미 아는 것과 연결하기

lag은 `kafka-consumer-groups.sh --describe`의 LAG 칸과 같은 계산이다. 차이는 한 번 찍고 끝나는 게 아니라 10초마다 찍어서 쌓는다는 것뿐이다.

| 지표 이름 | 뜻 | Kafka에서 가져오는 방법 |
|---|---|---|
| `kafka_topic_partition_current_offset` | 파티션의 끝 오프셋 | ListOffsets(latest). 컨슈머 입장의 끝이라 high watermark다 |
| `kafka_consumergroup_current_offset` | 그룹이 **커밋한** 오프셋 | OffsetFetch (`__consumer_offsets`에 저장된 값) |
| `kafka_consumergroup_lag` | 위 두 값의 차이 | exporter가 뺀다 |

컨슈머가 꺼져 있어도 커밋 기록은 `__consumer_offsets`에 남아 있으므로 lag은 계속 계산된다. 그래서 컨슈머를 멈추면 lag이 오르는 게 보인다.

PromQL 맛보기 (http://localhost:9090 의 Query 칸에 붙여 넣는다):

```
kafka_consumergroup_lag{consumergroup="raw-loader"}                                  # 파티션별 lag
sum(rate(kafka_topic_partition_current_offset{topic="wiki.recentchange"}[1m]))       # 초당 들어오는 메시지 수
sum(rate(kafka_consumergroup_current_offset{consumergroup="raw-loader"}[1m]))        # 초당 커밋되는 메시지 수
```

`rate(x[1m])`는 "최근 1분 동안 x가 초당 얼마씩 늘었나"다. 오프셋은 계속 커지기만 하는 값이라, 늘어난 속도가 곧 처리량이다.

### 실험 순서

돌리기 전에 아래 질문의 답을 `experiments/README.md` 0번 줄에 먼저 적는다.

1. 컨슈머가 정상으로 돌 때 `raw-loader`의 lag 그래프는 어떤 모양일까? 힌트: 이 lag은 컨슈머가 **읽은** 위치가 아니라 **커밋한** 위치로 계산한다. 우리 컨슈머는 언제 커밋하나?
2. 컨슈머를 5분 멈추면 lag은 얼마까지 오를까? 힌트: 프로듀서는 초당 약 30건을 보낸다.
3. 다시 켜면 lag이 0 근처로 돌아오기까지 얼마나 걸릴까? 따라잡는 동안 flush는 2,000건 조건과 60초 조건 중 어느 쪽으로 걸릴까?

```bash
docker compose up -d
open http://localhost:3000                  # Kafka Exporter Overview 대시보드가 바로 열린다
.venv/bin/python ingest/producer.py          # 터미널 1: 계속 켜 둔다
.venv/bin/python ingest/consumer.py          # 터미널 2: 5분쯤 정상 상태를 본다
# 터미널 2에서 Ctrl+C → 남은 batch를 넣고 커밋한 뒤 끝난다. 시각을 적어 둔다
# 5분 동안 "Lag by Consumer Group" 그래프를 본다
.venv/bin/python ingest/consumer.py          # 다시 켜고 lag이 줄어드는 걸 본다. 시각을 적어 둔다
```

보면 좋은 패널:

- **Lag by Consumer Group**: 이 실험의 주인공.
- **Message in per second**: 프로듀서 속도. 컨슈머를 멈춰도 이 값은 그대로여야 한다 (Kafka가 둘 사이를 끊어 주는 완충 장치라서).
- **Message consume per minute**: 커밋 기준 처리량. 따라잡는 동안 평소보다 높아진다.

그래프 오른쪽 위의 시간 범위를 실험 구간으로 좁히고 스크린샷을 `experiments/`에 남긴다.

### 설정에서 바꾼 것과 이유

- **Kafka 리스너를 하나 더 열었다** (`DOCKER://kafka:19092`). 클라이언트는 처음 접속한 뒤 브로커가 광고하는 주소로 다시 붙는다.
  기존 광고 주소 `localhost:9092`는 맥에서는 맞지만 kafka-exporter 컨테이너 안에서는 자기 자신을 가리킨다.
  그래서 맥용(`PLAINTEXT`, localhost:9092)과 컨테이너용(`DOCKER`, kafka:19092)을 따로 광고한다.
- **Kafka 데이터를 이름 있는 볼륨에 둔다** (`kafkadata`). 전에는 데이터가 컨테이너 안(`/tmp/kafka-logs`)에 있어서,
  compose 설정을 바꿔 컨테이너가 다시 만들어지면 토픽과 커밋 오프셋이 함께 사라지는 구조였다.
  2026-10-29에 옮길 때 기존 메시지 4,956건과 `raw-loader` 오프셋을 볼륨으로 복사해서 그대로 살렸다.
- **kafka-exporter에 `--kafka.version=3.9.0`을 준다.** Kafka 4.0부터 아주 오래된 요청 버전을 받지 않아서, 기본값(2.0.0)보다 높게 잡았다.
- **대시보드는 grafana.com의 공개 템플릿 7589번(Kafka Exporter Overview)이다.** 바꾼 것은 데이터 소스 자리표시자를 고정 uid로 바꾼 것,
  대시보드 uid 고정, 기본 시간 범위 30분과 10초 새로 고침뿐이다. Grafana 12가 옛날 graph 패널을 time series 패널로 자동 변환해서 그린다.
- Grafana 화면에서 고친 내용은 컨테이너를 다시 만들면 사라진다. 남기려면 대시보드의 Export → JSON을 `monitoring/grafana/dashboards/`에 저장한다.
