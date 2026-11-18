-- 한 행 = (날짜, 위키)

{% if var('daily_from_hourly', false) %}

-- 틀린 방법 (실험용): 시간별 마트를 더한다.
-- edit_count는 맞지만, 여러 시간에 걸쳐 편집한 사람이 시간마다 한 번씩 세어져 editor_count가 부풀어 오른다.
-- 실행: dbt run --vars '{daily_from_hourly: true}'
--
-- h.edit_count처럼 테이블 이름을 붙인 이유: ClickHouse에서 sum(edit_count) as edit_count라고 쓰면
-- 안쪽 edit_count가 컬럼이 아니라 바깥 별칭을 가리키게 될 수 있다.
select
    toDate(h.event_hour)        as event_date,
    h.wiki                      as wiki,
    sum(h.edit_count)           as edit_count,
    sum(h.editor_count)         as editor_count
from {{ ref('fct_edits_hourly') }} as h
group by event_date, wiki

{% else %}

-- 맞는 방법: 편집자 수는 staging에서 그날 기준으로 다시 센다.
select
    toDate(event_ts)            as event_date,
    wiki,
    count()                     as edit_count,
    count(distinct user_name)   as editor_count
from {{ ref('stg_recentchange') }}
where is_article_edit
group by event_date, wiki

{% endif %}
