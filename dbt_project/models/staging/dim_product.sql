{{ config(materialized='table') }}

SELECT DISTINCT
    stock_code,
    description
FROM {{ ref('stg_online_retail') }}