-- 시간별 mart의 grain은 (event_hour, wiki)다. 같은 키가 둘 이상이면 incremental 교체가 잘못된 것이다.

select
    event_hour,
    wiki,
    count() as row_count
from {{ ref('fct_edits_hourly') }}
group by event_hour, wiki
having row_count > 1
