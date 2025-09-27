{{ config(materialized='table') }}

SELECT
    ROW_NUMBER() OVER (ORDER BY driver_ratings DESC) AS driver_id,
    driver_ratings::DECIMAL AS driver_ratings,
    vehicle_type
FROM {{ source('stg', 'uber_ride') }}
GROUP BY driver_ratings, vehicle_type
