-- Макрос для загрузки данных из источника на основе дельты(дата на источнике - дата в хранилище)
{% macro delta_load(table_name) %}

-- Сформируем запрос на получение последеней актуальной даты в целевой таблице
{% set last_load_date_query %}
select last_datetime_dds 
from {{ source('postgres_db_staging', 'table_load_info') }} 
where table_name = '{{ table_name }}'
{% endset %}

-- Выполним запрос и сохраним необходимую дату в переменную
{% if execute %}
{% set last_load_date = run_query(last_load_date_query).columns[0].values()[0] %}
{% endif %}

-- выполним загрузку дельты с истоника
select *
from {{ source('postgres_db_source', table_name) }}
where calday > '{{ last_load_date }}'

{% endmacro %}