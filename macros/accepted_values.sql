-- Modified the source code for test_accepted_values; removed 'defailt__' from macro name  
{% macro test_accepted_values(model, values) %}

{% set column_name = kwargs.get('column_name', kwargs.get('field')) %}
{% set quote_values = kwargs.get('quote', True) %}

with all_values as (

    select distinct
        {{ column_name }} as value_field

    from {{ model }}

),

validation_errors as (

    select
        value_field

    from all_values
    where value_field not in (
        {% for value in values -%}
            {% if quote_values -%}
            '{{ value }}'
            {%- else -%}
            {{ value }}
            {%- endif -%}
            {%- if not loop.last -%},{%- endif %}
        {%- endfor %}
    )
),
-- Modified to not to fail but just warn on extra value found
vals as (
    select ','| value_field
from validation_errors 
)

select * from vals

{% endmacro %}

