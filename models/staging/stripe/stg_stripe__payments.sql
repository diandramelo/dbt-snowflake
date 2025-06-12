WITH payments AS (
    SELECT * FROM {{ source('stripe', 'payment') }}
)

SELECT
      id
    , orderid AS order_id
    , paymentmethod AS payment_method
    , status
    -- amount is stored in cents, convert it to dollars
    , {{ cents_to_dollars('amount', 4) }} as amount
    , created AS created_at
    , _batched_at
FROM payments