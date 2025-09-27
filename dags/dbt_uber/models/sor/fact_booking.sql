{{ config(materialized='table') }}

WITH customer_dim AS (
    SELECT * FROM {{ ref('dim_customer') }}
),
driver_dim AS (
    SELECT * FROM {{ ref('dim_driver') }}
)

SELECT
    f.booking_id::VARCHAR AS booking_id,
    c.customer_id,
    d.driver_id,
    f.booking_date::DATE AS booking_date,
    f.booking_time::TIME AS booking_time,
    f.vehicle_type,
    f.pickup_location,
    f.drop_location,
    NULLIF(f.avg_vtat, '')::DECIMAL AS avg_vtat,
    NULLIF(f.avg_ctat, '')::DECIMAL AS avg_ctat,
    NULLIF(f.cancelled_rides_by_customer, '')::DECIMAL AS cancelled_rides_by_customer,
    f.reason_for_cancelling_by_customer,
    NULLIF(f.cancelled_rides_by_driver, '')::DECIMAL AS cancelled_rides_by_driver,
    f.driver_cancellation_reason,
    NULLIF(f.incomplete_rides, '')::DECIMAL AS incomplete_rides,
    f.incomplete_rides_reason,
    NULLIF(f.booking_value, '')::DECIMAL AS booking_value,
    NULLIF(f.ride_distance, '')::DECIMAL AS ride_distance,
    d.driver_ratings,
    c.customer_rating,
    f.payment_method
FROM {{ source('stg', 'uber_ride') }} f
LEFT JOIN customer_dim c
    ON f.customer_id = c.customer_id
LEFT JOIN driver_dim d
    ON f.driver_ratings::DECIMAL = d.driver_ratings
    AND f.vehicle_type = d.vehicle_type
    AND f.driver_cancellation_reason = d.driver_cancellation_reason
