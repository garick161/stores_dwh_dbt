{% macro is_not_updated(table_name) %}

{% set sql %}
    select not load_to_dds
    from {{ source('postgres_db_staging', 'table_load_info') }}
    where table_name = '{{ table_name }}'
{% endset %}

{% set res = run_query(sql).columns[0].values()[0] %}
{{ return(res) }}

{% endmacro %}