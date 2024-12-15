-- Полная загрузка таблицы promos в слой DDS
{{ config(schema='dds', materialized='table', post_hook="{{ update_load_info_after_dds('promos') }}")}}
select
	promo_id::char(32),
    {{ dbt_utils.generate_surrogate_key(['name', 'promo_type', 'material', 'discount']) }} as hash_diff,
	"name"::text,
	promo_type::char(3),
	material::bigint,
	discount::smallint
from {{ ref('promos_stg') }}