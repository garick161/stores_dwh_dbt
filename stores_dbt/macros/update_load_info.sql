{% macro update_load_info(table_name) %}
update {{ source('postgres_db_staging', 'table_load_info') }}
set current_load_datetime = (select max(calday) from {{ this }}),
    load_to_dds = false,
    count_rows = (select count(*) from {{ this }})
where table_name = '{{ table_name }}';
{% endmacro %}