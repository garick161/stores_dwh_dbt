{{ config(schema='staging', materialized='table', post_hook="{{ update_load_info('stores') }}") }}

-- Полная загрузка таблицы stores
{{ full_load('stores') }}  