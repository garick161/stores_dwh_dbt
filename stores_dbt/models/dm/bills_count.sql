{{ config(schema='dm', materialized='ephemeral') }}
	
SELECT plant, count(billnum) AS bills_count
FROM {{ ref('bills_head') }} bh
WHERE calday >= '{{ var('date_from') }}' AND calday < '{{ var('date_to') }}'
GROUP BY plant