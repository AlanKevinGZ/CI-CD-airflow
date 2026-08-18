{{ config(materialized='table') }}

WITH customer_country AS (
    SELECT
        customer_id,
        country,
        COUNT(*) AS total_transactions
    FROM {{ ref('stg_online_retail') }}
    WHERE customer_id IS NOT NULL
    GROUP BY customer_id, country
),

ranked AS (
    SELECT
        customer_id,
        country,
        total_transactions,
        ROW_NUMBER() OVER (
            PARTITION BY customer_id
            ORDER BY total_transactions DESC
        ) AS rn
    FROM customer_country
)

SELECT
    customer_id,
    country
FROM ranked
WHERE rn = 1