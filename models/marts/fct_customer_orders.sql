-- Import CTEs
WITH orders AS (
    SELECT * FROM {{ ref("stg_jaffle_shop__orders") }} 
)

, customers AS (
    SELECT * FROM {{ ref("stg_jaffle_shop__customers") }}
)

, payments AS (
    SELECT * FROM {{ ref("stg_stripe__payments") }}
)


-- Logical CTEs
, completed_payments AS (
    SELECT 
          orderid AS order_id
        , MAX(created) AS payment_finalized_date
        , SUM(amount) / 100.0 AS total_amount_paid
    FROM payments
        WHERE status <> 'fail'
        GROUP BY 1
)

, paid_orders AS (
    SELECT 
          orders.id AS order_id
        , orders.user_id AS customer_id
        , orders.order_date AS order_placed_at
        , orders.status AS order_status

        , completed_payments.total_amount_paid
        , completed_payments.payment_finalized_date

        , customers.first_name AS customer_first_name
        , customers.last_name AS customer_last_name
    FROM orders
    LEFT JOIN completed_payments ON orders.id = completed_payments.order_id
    LEFT JOIN customers ON orders.user_id = customers.id 
)


-- Final CTE

, final AS (   
    SELECT
        paid_orders.*
        -- sales transaction sequence
        ROW_NUMBER() OVER(ORDER BY order_id) AS transaction_seq,

        -- customer sales sequence
        ROW_NUMBER() OVER(PARTITION BY customer_id ORDER BY order_id) AS customer_sales_seq,

        -- new vs returning customer
        CASE  
        WHEN RANK() OVER ( PARTITION BY customer_id ORDER BY order_placed_at, order_id ) = 1 THEN 'new'
        ELSE 'return'
        END AS nvsr,

        -- customer lifetime value
        SUM(total_amount_paid) OVER (
        PARTITION BY customer_id
        ORDER BY order_placed_at
        ) AS customer_lifetime_value,

        -- first day of sale
        FIRST_VALUE(order_placed_at) OVER (
        PARTITION BY customer_id
        ORDER BY order_placed_at
        ) AS fdos

    FROM paid_orders
)


-- Simple SELECT statement

SELECT * FROM final
    ORDER BY order_id