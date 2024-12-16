{{ config(schema='dm', materialized='view') }}

SELECT plant, sum(quantity) AS total_traffic 
FROM {{ ref('traffic') }}
WHERE calday >= '{{ var('date_from') }}' AND calday < '{{ var('date_to') }}'
GROUP BY plant
