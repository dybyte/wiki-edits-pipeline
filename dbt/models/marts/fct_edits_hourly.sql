-- 한 행 = (시간, 위키). 위키백과 일반 문서 편집만 센다.

{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    engine='MergeTree()',
    partition_by='toDate(event_hour)',
    order_by='event_hour',
    on_schema_change='fail'
) }}

-- 평소 실행은 마지막 적재 날짜와 그 전날을 통째로 다시 계산한다.
-- 늦게 도착한 이벤트를 흡수하면서도 전체 이력을 매번 읽지 않기 위한 1일 lookback이다.
-- 오래된 날짜를 명시적으로 다시 처리할 때는 --vars '{reprocess_from: YYYY-MM-DD}'를 준다.

select
    toStartOfHour(event_ts)                     as event_hour,
    wiki,
    count()                                     as edit_count,
    countIf(event_type = 'new')                 as new_page_count,
    round(countIf(is_bot) / count(), 3)         as bot_ratio,
    sum(byte_delta)                             as bytes_delta,
    count(distinct user_name)                   as editor_count
from {{ ref('stg_recentchange') }}
where is_article_edit
{% if var('reprocess_from', none) %}
  and event_ts >= toDateTime('{{ var("reprocess_from") }}', 'UTC')
  {% if var('reprocess_until', none) %}
  and event_ts < toDateTime('{{ var("reprocess_until") }}', 'UTC')
  {% endif %}
{% elif is_incremental() %}
  and event_ts >= coalesce(
    (select toStartOfDay(max(event_hour)) from {{ this }}) - interval 1 day,
    toDateTime('1970-01-01 00:00:00', 'UTC')
  )
{% endif %}
group by event_hour, wiki
