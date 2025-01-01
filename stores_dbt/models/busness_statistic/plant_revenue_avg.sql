-- Среднее и стандартное отклонение выручки по магазинам за день
{{ config(schema='busness_statistic', materialized='table')}}

select plant, 
       round(avg(daily_revenue), 2) as avg_revenue, 
       round(2 * stddev_pop(daily_revenue), 2) as double_std_revenue
from (
    select bh.plant, bh.calday, sum(bi.rpa_sat) as daily_revenue
    from {{ ref('bills_head') }} bh 
    join {{ ref('bills_item') }} bi 
    on bh.billnum = bi.billnum and bh.calday = bi.calday 
    group by bh.plant, bh.calday
) q
group by plant
