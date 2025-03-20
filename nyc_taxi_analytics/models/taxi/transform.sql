{{ config(
    materialized='external',
    location='output/trips_2024_transformed.parquet',
    format='parquet'
) }}

WITH source_data AS (
    SELECT * EXCLUDE (VendorID, RateCodeID)
    FROM {{ source('tlc', 'transform_file') }}
),

filtered_data AS (
    SELECT *
    FROM source_data
    WHERE
        passenger_count > 0
        AND Store_and_fwd_flag = 'N'
        AND trip_distance > 0
        AND Payment_type IN (1, 2, 3)
        AND tpep_dropoff_datetime > tpep_pickup_datetime
        AND tip_amount >= 0
        AND total_amount > 0
),

transformed_data AS (
    SELECT
        -- Correction : éviter le doublon `passenger_count`
        CAST(passenger_count AS BIGINT) AS passenger_count,

        -- Traduction des méthodes de paiement
        CASE
            WHEN Payment_type = 1 THEN 'Credit card'
            WHEN Payment_type = 2 THEN 'Cash'
            WHEN Payment_type = 3 THEN 'No charge'
        END AS payment_method,

        -- Calcul de la durée en minutes
        DATE_DIFF('minute', tpep_pickup_datetime, tpep_dropoff_datetime) AS trip_duration_minutes,

        -- Ajout des colonnes originales restantes
        * EXCLUDE (passenger_count, Payment_type)
    FROM filtered_data
),

final_data AS (
    SELECT *,
        CAST(tpep_pickup_datetime AS DATE) AS pickup_date,
        CAST(tpep_dropoff_datetime AS DATE) AS dropoff_date,

    FROM transformed_data
    WHERE 
        -- Garder uniquement les trajets qui commencent et finissent en 2024
        pickup_date >= '2024-01-01' AND pickup_date < '2025-01-01'
        AND dropoff_date >= '2024-01-01' AND dropoff_date < '2025-01-01'
)

SELECT * EXCLUDE (pickup_date, dropoff_date) FROM final_data
WHERE trip_duration_minutes > 0