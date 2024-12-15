{% macro update_load_info_after_dds(table_name) %}

-- Для таблиц с полной загрузкой обновляем только флаг успешной загрузки в хранилище
{% if table_name in ['stores', 'promo_types', 'promos'] %}
    update {{ source('postgres_db_staging', 'table_load_info') }}
    set load_to_dds = true
    where table_name = '{{ table_name }}';
-- Для таблиц фактов обновляем еще последнию дату в хранилище 
{% else %}
    update {{ source('postgres_db_staging', 'table_load_info') }}
    set last_datetime_dds = (select last_datetime_source
                            from {{ source('postgres_db_staging', 'table_load_info') }}
                            where table_name = '{{ table_name }}'),
        load_to_dds = true,
        count_rows = (select count(*) from {{ this }})
    where table_name = '{{ table_name }}';
{% endif %}

{% endmacro %}
