{{
    config(
        materialized='incremental',
        unique_key='id',
        query_tag = 'dbt_special',
        incremental_strategy='delete+insert'
          )
}}

with temp_table as (
    select s.*,concat(s.id,s.seq_no) as u_key
    from SCD1_SRC s
    -- Adding condition to allow SCD 1 load the latest records for a range of dates
    join (select id,max(commit_date) as max_date from SCD1_SRC where commit_date between '2020-02-03' and '2020-02-03' group by id ) as t
    on t.id= s.id and s.commit_date = t.max_date
    where commit_date between '2020-02-01' and '2020-02-03'
)
 
 select * from temp_table

 {% if is_incremental() %}

 where id in  (select id from temp_table)
 {% endif %}