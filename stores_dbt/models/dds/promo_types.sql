-- Инкрементальная загрузка дельты для таблицы promo_types в слой DDS
{{ config(schema='dds', materialized='table', post_hook="{{ update_load_info_after_dds('promo_types') }}")}}
select
	promo_type::char(3),
	txt::text
from {{ ref('promo_types_stg') }}