{{ config(schema='staging', materialized='table', post_hook="{{ update_load_info('bills_item') }}") }}

-- Загрузка дельты для таблицы bills_item
{{ delta_load('bills_item') }}  
