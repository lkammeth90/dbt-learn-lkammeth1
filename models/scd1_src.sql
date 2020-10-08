{{
    config(
        materialized='incremental',
        unique_key='commit_date',
        query_tag = 'dbt_special',
        incremental_strategy='delete+insert'
    )
}}

select seq_no,id,name,color,commit_date, current_timestamp(2) as load_timestamp
from TEMP_SDC1_SRC

{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  where commit_date >= (select max(commit_date) from {{ this }})

{% endif %}