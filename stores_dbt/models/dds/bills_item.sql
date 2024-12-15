{{ 
    config(
        schema='dds', 
        materialized='incremental', 
        incremental_strategy='append',  
        post_hook="{{ update_load_info_after_dds('bills_item') }}")
 }}

select
	billnum::bigint,
	billitem::bigint,
	material::bigint,
	qty::bigint,
	netval::numeric(17, 2),
	tax::numeric(17, 2),
	rpa_sat::numeric(17, 2),
	calday 
from {{ ref('staging.bills_item') }}
{% if is_incremental() %}
{% endif %}

