{% macro update_load_info(table_name) %}

-- Для таблиц с полной загрузкой обновляем только текущее количество загруженных записей 
{% if table_name in ['stores', 'promo_types', 'promos'] %}
    update {{ source('postgres_db_staging', 'table_load_info') }}
    set load_to_dds = false,
        count_rows = (select count(*) from {{ this }})
    where table_name = '{{ table_name }}';
-- Для таблиц фактов обновляем еще последнию дату на источнике 
{% else %}
    update {{ source('postgres_db_staging', 'table_load_info') }}
    set last_datetime_source = (select max(calday) from {{ this }}),
        load_to_dds = false,
        count_rows = (select count(*) from {{ this }})
    where table_name = '{{ table_name }}';
{% endif %}

{% endmacro %}