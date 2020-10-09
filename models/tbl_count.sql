
{% set val=get_tbl_count(src_tbl,src_col, tgt_tbl,tgt_col) %}

with vals as (
    select {{ val }}

)

select * from vals