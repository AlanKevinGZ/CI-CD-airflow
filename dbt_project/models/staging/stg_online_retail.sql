
-- cuando ejecutes este modelo, créalo como una tabla física en el warehouse

{{ config(materialized='table') }}

-- referenciar una tabla que ya existe en tu Data Warehouse
-- {{ source('raw', 'raw_online_retail') }}

-- Leer la tabla RAW creada por Airflow
-- select * from {{ source('raw', 'raw_online_retail') }}

select
    InvoiceNo AS invoice_no,StockCode AS stock_code,
    Description AS description,Quantity AS quantity,
    InvoiceDate AS invoice_date,UnitPrice AS unit_price,
    CAST(CustomerID AS INT64) AS customer_id,
    Country AS country
    from {{ source('raw', 'raw_online_retail') }}
    WHERE Quantity > 0 AND UnitPrice > 0 AND not starts_with(cast(InvoiceNo as string), 'C')
