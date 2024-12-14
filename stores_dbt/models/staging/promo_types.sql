{{ config(schema='staging', materialized='table', post_hook="{{ update_load_info('promo_types') }}") }}

-- Полная загрузка таблицы promo_types
{{ full_load('promo_types') }}  