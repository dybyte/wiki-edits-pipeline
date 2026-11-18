{#
  dbt는 기본으로 스키마(ClickHouse에서는 데이터베이스) 이름을 "analytics_staging"처럼 붙여서 만든다.
  이 매크로는 dbt_project.yml에 적은 이름(staging, marts)을 그대로 쓰게 한다.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
