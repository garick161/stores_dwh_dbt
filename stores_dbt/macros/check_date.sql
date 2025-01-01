-- тест для проверки, что дата больше текущей (невалидные/ошибочные данные)
{% test check_date(model, column_name) %}

    select *
    from {{ model }}
    where {{ column_name }} >= '{{ var("date_to") }}'

{% endtest %}