{{config(materialized='table') }}

SELECT DISTINCT
    customer_id,
    customer_rating::DECIMAL AS customer_rating
FROM {{ source('stg', 'uber_ride') }}
WHERE customer_id IS NOT NULL
