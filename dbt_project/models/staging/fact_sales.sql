{{ config(materialized='table') }}

{{ config(materialized='table') }}

SELECT
    s.invoice_no,
    s.customer_id,
    s.stock_code,
    CAST(s.invoice_date AS DATE) AS date,
    s.quantity,
    s.unit_price,
    s.quantity * s.unit_price AS revenue
FROM {{ ref('stg_online_retail') }} AS s

INNER JOIN {{ ref('dim_customer') }} AS c
    ON s.customer_id = c.customer_id

INNER JOIN {{ ref('dim_product') }} AS p
    ON s.stock_code = p.stock_code

INNER JOIN {{ ref('dim_date') }} AS d
    ON CAST(s.invoice_date AS DATE) = d.date