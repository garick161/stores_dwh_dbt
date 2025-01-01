-- тест для проверки нулевого трафика магазина за день
{% test check_plant_zero_traffic(model) %}

    SELECT s.plant, t.calday
    FROM {{ ref('stores') }} s 
    LEFT JOIN 
    (SELECT plant, calday, sum(quantity) AS daily_traffic
    FROM {{ model }}
    GROUP BY plant, calday) t
    ON s.plant = t.plant
    WHERE t.daily_traffic IS null

{% endtest %}