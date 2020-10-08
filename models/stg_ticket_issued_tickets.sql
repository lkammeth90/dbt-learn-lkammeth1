select status as issued_ticket_status, 
id as issueed_ticket_id,
order_id,
event_id
from {{source('ticket_tailor','issued_tickets')}}

