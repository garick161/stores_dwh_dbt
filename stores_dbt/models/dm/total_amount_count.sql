{{ config(schema='dm', materialized='ephemeral') }}

select bh.plant, sum(bi.rpa_sat) as total_amount, sum(bi.qty) as total_item_count
from {{ ref('bills_head') }} bh
join {{ ref('bills_item') }} bi
on bh.billnum = bi.billnum and bh.calday = bi.calday
where bh.calday >= '{{ var('date_from') }}' and bh.calday < '{{ var('date_to') }}'
group by bh.plant

