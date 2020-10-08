select id as event_id,
status as event_status, 
total_orders,
total_issued_tickets
from {{source('ticket_tailor','events')}}

