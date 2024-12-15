{{ config(schema='staging', materialized='table', post_hook="{{ update_load_info('promos') }}") }}

-- Полная загрузка таблицы promos
{{ full_load('promos') }}  