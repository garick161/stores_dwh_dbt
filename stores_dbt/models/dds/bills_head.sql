{{ 
    config(
        schema='dds', 
        materialized='incremental', 
        incremental_strategy='append',  
        post_hook="{{ update_load_info_after_dds('bills_head') }}")
 }}

{% if execute %}
    {% set flag =  is_not_updated('bills_head') %}
{% endif %}

-- Если `flag` = Fasle, значит данные уже обновлены и нет необходимости загружать. 
-- Если `flag` = True, значит данные последней дельты не загружены. Произойдет добавление новых записей в хранилище
-- И с помощью 'post_hook' будут обновлены записи в информационной таблице `table_load_info`
select
    billnum::bigint,
    plant::char(4),
    calday::date
from {{ ref('bills_head_stg') }}
{% if is_incremental() %}
where '{{ flag }}'
{% endif %}


