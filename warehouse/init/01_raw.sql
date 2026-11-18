-- raw 계층: 받은 이벤트를 그대로 보관한다. 가공은 전부 dbt에서 한다.
--
-- 중복을 여기서 막지 않는다. ClickHouse는 primary key가 있어도 유일성을 보장하지 않고,
-- BigQuery·Snowflake·Redshift 같은 다른 DW도 마찬가지다.
-- 컨슈머가 같은 이벤트를 두 번 넣으면 raw에는 두 줄이 남고, staging에서 하나로 만든다.
-- 그래야 실험 1에서 "raw에 생긴 중복 수"와 "마트 오차 0"을 따로 셀 수 있다.
create database if not exists raw;

create table if not exists raw.raw_recentchange
(
    event_id        String,                              -- meta.id (이벤트 고유 ID)
    event_ts        DateTime64(3, 'UTC'),                -- meta.dt (편집이 일어난 시각)
    payload         String,                              -- 받은 JSON 원본 전체
    kafka_partition Nullable(Int32),                     -- 내 Kafka에서 읽은 위치 (샘플 적재 때는 비어 있음)
    kafka_offset    Nullable(Int64),
    ingested_at     DateTime64(3, 'UTC') default now64(3) -- DW에 들어온 시각
)
engine = MergeTree
partition by toDate(event_ts)   -- 날짜별로 나눠 저장한다. 백필 때 하루치만 지우고 다시 넣기 쉽다
order by (event_ts, event_id);  -- 디스크에 이 순서로 정렬해 둔다. 시간 범위로 읽을 때 빠르다
