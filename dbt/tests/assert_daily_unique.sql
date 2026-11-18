-- 일별 mart의 grain은 (event_date, wiki)다.

select
    event_date,
    wiki,
    count() as row_count
from {{ ref('fct_edits_daily') }}
group by event_date, wiki
having row_count > 1
