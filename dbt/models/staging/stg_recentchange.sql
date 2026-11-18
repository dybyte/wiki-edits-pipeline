-- staging: raw의 JSON을 컬럼으로 펼치고 정리만 한다. 집계는 하지 않는다.
-- 한 행 = 이벤트 하나
--
-- 컨슈머가 ClickHouse insert 뒤 Kafka offset commit 전에 죽으면 같은 이벤트가 raw에 다시 들어온다.
-- raw는 수신 기록을 그대로 보존하고, staging에서 event_id별 가장 최근 적재 한 건만 남긴다.

with source as (
    select *
    from {{ source('raw', 'raw_recentchange') }}
    order by ingested_at desc, kafka_partition desc, kafka_offset desc
    limit 1 by event_id
),

renamed as (
    select
        event_id,
        event_ts,   -- raw 테이블에서 이미 UTC로 고정돼 있다: DateTime64(3, 'UTC')
        -- JSON에 키가 없으면 NULL이 되도록 Nullable로 꺼낸다 (Nullable이 아니면 빈 문자열이나 0이 된다)
        JSONExtract(payload, 'wiki', 'Nullable(String)')             as wiki,
        JSONExtract(payload, 'server_name', 'Nullable(String)')      as server_name,
        JSONExtract(payload, 'type', 'Nullable(String)')             as event_type,
        JSONExtract(payload, 'namespace', 'Nullable(Int32)')         as namespace,
        JSONExtract(payload, 'title', 'Nullable(String)')            as page_title,
        JSONExtract(payload, 'user', 'Nullable(String)')             as user_name,
        JSONExtract(payload, 'bot', 'Nullable(Bool)')                as is_bot,
        JSONExtract(payload, 'minor', 'Nullable(Bool)')              as is_minor,
        JSONExtract(payload, 'length', 'old', 'Nullable(Int64)')     as len_old,
        JSONExtract(payload, 'length', 'new', 'Nullable(Int64)')     as len_new,
        JSONExtract(payload, 'log_type', 'Nullable(String)')         as log_type,
        JSONExtract(payload, 'log_action', 'Nullable(String)')       as log_action,
        JSONExtract(payload, '$schema', 'Nullable(String)')          as schema_version,
        JSONExtract(payload, 'meta', 'domain', 'Nullable(String)')   as meta_domain,
        ingested_at
    from source
)

select
    *,
    -- 새 문서는 len_old가 없으므로 0으로 본다
    coalesce(len_new, 0) - coalesce(len_old, 0)   as byte_delta,
    -- "편집 수"에 넣을 이벤트의 정의를 여기 한 곳에만 둔다:
    -- 위키백과의 일반 문서(namespace 0)를 고치거나 새로 만든 것
    coalesce(
        event_type in ('edit', 'new')
        and endsWith(server_name, '.wikipedia.org')
        and namespace = 0,
        0
    )                                              as is_article_edit
from renamed
-- canary: Wikimedia가 스트림 점검용으로 흘려보내는 테스트 이벤트. 편집 정보가 없다.
where coalesce(meta_domain, '') != 'canary'
