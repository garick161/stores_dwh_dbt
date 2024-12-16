-- Модель для формирования итоговой витрины
-- Название витрины формируется динамически по правилу:
-- order_date_from_date_to согласно переданным параметрам при запуске dbt модели

{% set view_name = 'order_' ~ var('date_from') ~ '_' ~ var('date_to') %}

{{ config(schema='dm', materialized='view', alias=view_name) }}

select tdcdi.plant, taic.total_amount, tdcdi.total_discount_amount,
		taic.total_amount - tdcdi.total_discount_amount as total_without_discount,
		taic.total_item_count,
		tcb.bills_count,
		traf.total_traffic,
		tdcdi.count_item_discount,
		round(100 * tdcdi.count_item_discount::decimal / taic.total_item_count, 1) as discount_item_rate,
		round(taic.total_item_count::decimal / tcb.bills_count, 2) as avg_items_in_bill,
		round(100 * tcb.bills_count::decimal / traf.total_traffic, 2) as conversion_rate,
		round(taic.total_amount::decimal / tcb.bills_count, 1) as avg_bill,
		case 
			when traf.total_traffic > 0 then round(taic.total_amount::decimal / traf.total_traffic, 1)
			else 0.0
		end as avg_revenue_per_customer
from {{ ref('total_amount_count') }} taic
join {{ ref('bills_count') }} tcb
on taic.plant = tcb.plant		
join {{ ref('total_discount_amount_count') }} tdcdi
on taic.plant = tdcdi.plant	
join {{ ref('total_traffic') }} traf
on taic.plant = traf.plant
