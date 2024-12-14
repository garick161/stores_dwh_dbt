{{ config(schema='staging', materialized='table', post_hook="{{ update_load_info('bills_head') }}") }}

-- Загрузка дельты для таблицы bills_head
{{ delta_load('bills_head') }}  
