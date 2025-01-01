-- тест для проверки выручки магазина за день
{% test check_daily_revenue(model) %}

with daily_revenue_table as (
    select bh.plant, bh.calday, sum(bi.rpa_sat) as daily_revenue
    from {{ ref('bills_head_stg') }} bh 
    join {{ ref('bills_item_stg') }} bi 
    on bh.billnum = bi.billnum and bh.calday = bi.calday 
    group by bh.plant, bh.calday
)

select drt.plant, drt.calday, drt.daily_revenue
from daily_revenue_table drt
join {{ ref('plant_revenue_avg') }} pra 
on drt.plant = pra.plant 
where drt.daily_revenue > pra.avg_revenue + double_std_revenue 
    or drt.daily_revenue < pra.avg_revenue - double_std_revenue

{% endtest %}