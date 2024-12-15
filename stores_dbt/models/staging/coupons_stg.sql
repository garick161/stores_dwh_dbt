{{ config(schema='staging', materialized='table', post_hook="{{ update_load_info('coupons') }}") }}

-- Загрузка дельты для таблицы coupons
{{ delta_load('coupons') }}  
