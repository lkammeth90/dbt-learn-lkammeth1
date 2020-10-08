select
    status as order_status, 
    refund_amount, 
    id as order_id,
    total as total_amount
from {{source('ticket_tailor','orders')}}
