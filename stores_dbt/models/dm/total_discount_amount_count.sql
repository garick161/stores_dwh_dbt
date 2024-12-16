{{ config(schema='dm', materialized='view') }}

select plant, sum(discount_amount) as total_discount_amount, count(*) as count_item_discount
from (
        select c.plant, 
                case 
                    when p.promo_type = '002' then round(p.discount * (bi.rpa_sat / bi.qty)  / 100, 2)
                    else p.discount
                end as discount_amount,
                row_number() over(partition by c.billnum, c.material, c.coupon_id order by bi.billitem) as rnk
        from {{ ref('coupons') }} c
        join {{ ref('promos') }} p 
        on c.promo_id = p.promo_id and c.material = p.material
        join {{ ref('bills_item') }} bi
        on c.billnum = bi.billnum and c.material = bi.material
        where bi.calday >= '{{ var('date_from') }}' and bi.calday < '{{ var('date_to') }}'
) q
where rnk = 1
group by plant