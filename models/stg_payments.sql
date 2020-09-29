select  id as customer_id,
orderid as order_id,
paymentmethod,
amount/100 as amount,status
from {{source('stripe','payment')}}