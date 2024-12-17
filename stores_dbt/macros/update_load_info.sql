{% macro update_load_info(table_name) %}

-- Для таблиц с полной загрузкой обновляем только текущее количество загруженных записей 
{% if table_name in ['stores', 'promo_types', 'promos'] %}
    update {{ source('postgres_db_staging', 'table_load_info') }}
    set load_to_dds = false,
        count_rows = (select count(*) from {{ this }})
    where table_name = '{{ table_name }}';
-- Для таблиц фактов обновляем еще последнию дату на источнике 
{% else %}
    {% if execute %}
        {% set sql_query %}
        select count(*) from {{ this }} 
        {% endset %}

        {% set count_rows_stg = run_query(sql_query).columns[0].values()[0] %}
        -- Защита от случайного повтоного обновления хранилища или когда на источнике новых данных не появилось
        -- Если таблица в staging слое не пустая, то обновляем метаинформацию в таблице 
        -- table_load_info: last_datetime_source и count_rows
        {% if count_rows_stg > 0 %}
            update {{ source('postgres_db_staging', 'table_load_info') }}
            set last_datetime_source = (select max(calday) from {{ this }}),
                load_to_dds = false,
                count_rows = '{{ count_rows_stg }}'
            where table_name = '{{ table_name }}';
        -- Если новых записей нет => целевая таблица в актуальном состоянии и last_datetime_source = last_datetime_dds
        {% else %}
            update {{ source('postgres_db_staging', 'table_load_info') }}
            set last_datetime_source = (select last_datetime_dds 
                                        from {{ source('postgres_db_staging', 'table_load_info') }}
                                        where table_name = '{{ table_name }}'),
                load_to_dds = true
            where table_name = '{{ table_name }}';
        {% endif %}
    {% endif %} 
{% endif %}

{% endmacro %}