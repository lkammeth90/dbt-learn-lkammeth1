-- Custom row count validation between source and taget tables defined in schema.yml
-- Sample to execute the test :- dbt test -m scd1_src --vars '{start_date: 2020-02-03, end_date: 2020-02-03}'
{% macro test_validate_src_tgt_row_count(model) %}

{% set src_tbl =  model  %}
{% set tgt_tbl = kwargs.get('compare_model', kwargs.get('arg')) %}
{% set src_col = kwargs.get('model_col', kwargs.get('arg')) %}
{% set tgt_col = kwargs.get('compare_model_col', kwargs.get('arg')) %}

{#-- Prevent querying of db in parsing mode. This works because this macro does not create any new refs. #}
{%- if not execute -%}
    {{ return('') }}
{% endif %}

with a as (
    select count(*) as count_a from {{ src_tbl }} where {{ src_col }}  between date('{{var('start_date')}}') and date('{{var('end_date')}}')
),
b as (
    select count(*) as count_b from {{ tgt_tbl }} where {{ tgt_col }} between date('{{var('start_date')}}') and date('{{var('end_date')}}')
),
final as (
    select abs(
            (select count_a from a) -
            (select count_b from b)
            )
        as diff_count
)
select diff_count as number from final
{% endmacro %}
