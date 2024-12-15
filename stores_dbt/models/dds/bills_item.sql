-- Инкрементальная загрузка дельты для таблицы bills_item в слой DDS
{{ 
    config(
        schema='dds', 
        materialized='incremental', 
        incremental_strategy='append',  
        post_hook="{{ update_load_info_after_dds('bills_item') }}")
 }}

{% if execute %}
    {% set flag =  is_not_updated('bills_item') %}
{% endif %}

-- Если `flag` = Fasle, значит данные уже обновлены и нет необходимости загружать. 
-- Если `flag` = True, значит данные последней дельты не загружены. Произойдет добавление новых записей в хранилище
-- И с помощью 'post_hook' будут обновлены записи в информационной таблице `table_load_info`
select
	billnum::bigint,
	billitem::bigint,
	material::bigint,
	qty::bigint,
	netval::numeric(17, 2),
	tax::numeric(17, 2),
	rpa_sat::numeric(17, 2),
	calday::date
from {{ ref('bills_item_stg') }}
{% if is_incremental() %}
where '{{ flag }}'
{% endif %}
