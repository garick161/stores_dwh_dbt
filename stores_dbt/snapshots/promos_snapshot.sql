{% snapshot promos_snapshot %}
{{ 
    config(
        target_schema='snapshots',
        strategy='check',
        unique_key='promo_id',
        check_cols=['hash_diff'],
        invalidate_hard_deletes=true
    )
}}

select * from {{ ref('promos') }}

{% endsnapshot %}