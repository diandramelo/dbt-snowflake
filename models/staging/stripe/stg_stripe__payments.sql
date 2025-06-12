WITH payments AS (
    SELECT * FROM {{ source('stripe', 'payment') }}
)

SELECT
      id
    , orderid AS order_id
    , paymentmethod AS payment_method
    , status
    , amount
    , created AS created_at
    , _batched_at
FROM payments