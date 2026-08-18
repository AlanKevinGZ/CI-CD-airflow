SELECT
    stock_code,
    description,
    COUNT(*) AS total
FROM {{ ref('dim_product') }}
GROUP BY
    stock_code,
    description
HAVING COUNT(*) > 1