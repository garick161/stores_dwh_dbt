{{ config(schema='staging', materialized='table', post_hook="{{ update_load_info('traffic') }}") }}

-- Загрузка дельты для таблицы traffic
{{ delta_load('traffic') }}  
