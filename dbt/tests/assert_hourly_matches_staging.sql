-- incremental 시간별 mart 전체가 staging에서 직접 집계한 값과 같은지 검사한다.

with expected as (
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
    group by event_hour, wiki
)

select
    coalesce(e.event_hour, h.event_hour) as event_hour,
    coalesce(e.wiki, h.wiki)             as wiki
from expected as e
full outer join {{ ref('fct_edits_hourly') }} as h
    on h.event_hour = e.event_hour and h.wiki = e.wiki
where h.edit_count != e.edit_count
   or h.new_page_count != e.new_page_count
   or h.bot_ratio != e.bot_ratio
   or h.bytes_delta != e.bytes_delta
   or h.editor_count != e.editor_count
   or e.wiki is null
   or h.wiki is null
