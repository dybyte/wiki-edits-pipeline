-- 일별 마트가 staging에서 직접 다시 센 값과 같은지 검사한다.
-- 결과가 0행이면 통과, 1행이라도 나오면 실패. 나온 행이 바로 틀린 데이터다.

with expected as (
    select
        toDate(event_ts)            as event_date,
        wiki,
        count()                     as edit_count,
        count(distinct user_name)   as editor_count
    from {{ ref('stg_recentchange') }}
    where is_article_edit
    group by event_date, wiki
)

select
    -- 한쪽에만 있는 행이면 다른 쪽 칸은 ClickHouse 기본값(날짜 1970-01-01, 숫자 0)으로 채워진다
    if(e.edit_count = 0, d.event_date, e.event_date)    as event_date,
    coalesce(e.wiki, d.wiki)                            as wiki,
    d.edit_count                                        as mart_edit_count,
    e.edit_count                                        as expected_edit_count,
    d.editor_count                                      as mart_editor_count,
    e.editor_count                                      as expected_editor_count
from expected as e
full outer join {{ ref('fct_edits_daily') }} as d
    on d.event_date = e.event_date and d.wiki = e.wiki
-- 값이 다르거나, 한쪽에 행이 없어서 0이 채워진 경우가 모두 걸린다
where d.edit_count != e.edit_count
   or d.editor_count != e.editor_count
