{% test not_empty(model) %}

--Test fails if sum of 'test_value' field defined below does not equal zero.
{{ config(fail_calc = "sum(test_value)") }}

--Only run this test during "execution" phase of dbt test (do not run in "parsing" phase).
{% if not execute %}
    {{ return(none) }}
{% endif %}

select
    case 
        when count(*) > 0 then 0
        else 1
    end as test_value
from {{ model }}

{% endtest %}