{{ config(materialized='table') }}

SELECT DISTINCT
    CAST(invoice_date AS DATE) AS date,
    EXTRACT(YEAR FROM invoice_date) AS year,
    EXTRACT(MONTH FROM invoice_date) AS month,
    EXTRACT(DAY FROM invoice_date) AS day
FROM {{ ref('stg_online_retail') }}
