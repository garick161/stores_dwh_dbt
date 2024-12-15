-- Инкрементальная загрузка дельты для таблицы traffic в слой DDS
{{ 
    config(
        schema='dds', 
        materialized='incremental', 
        incremental_strategy='append',  
        post_hook="{{ update_load_info_after_dds('traffic') }}")
 }}

{% if execute %}
    {% set flag =  is_not_updated('traffic') %}
{% endif %}

-- Если `flag` = Fasle, значит данные уже обновлены и нет необходимости загружать. 
-- Если `flag` = True, значит данные последней дельты не загружены. Произойдет добавление новых записей в хранилище
-- И с помощью 'post_hook' будут обновлены записи в информационной таблице `table_load_info`
select
	plant::char(4),
	calday::date,
	"time"::char(6),
	frame_id::char(10),
	quantity::int
from {{ ref('traffic_stg') }}
{% if is_incremental() %}
where '{{ flag }}'
{% endif %}
