-- Макрос для полной загрузки данных из источника
{% macro full_load(table_name) %}

select *
from {{ source('postgres_db_source', table_name) }}

{% endmacro %}