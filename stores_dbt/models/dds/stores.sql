-- Инкрементальная загрузка дельты для таблицы stores в слой DDS
{{ config(schema='dds', materialized='table', post_hook="{{ update_load_info_after_dds('stores') }}")}}
select
	plant::char(4),
	txt::text
from {{ ref('stores_stg') }}
