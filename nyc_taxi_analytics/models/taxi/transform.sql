{{ config(
    materialized='external',
    location='output/trips_2023_01_transformed.parquet',
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
        CAST(CAST(tpep_pickup_datetime AS DATE) AS TEXT) AS pickup_date,
        CAST(CAST(tpep_dropoff_datetime AS DATE) AS TEXT) AS dropoff_date,

        -- Utilisation de la variable Jinja pour l’année
        CAST({{ var('year') }} AS BIGINT) AS year
    FROM transformed_data
    WHERE
        -- Garder uniquement les trajets qui commencent et finissent dans l'année cible
        ((pickup_date BETWEEN CAST(year AS TEXT) || '-01-01' AND CAST(year AS TEXT) || '-12-31')
        AND (dropoff_date BETWEEN CAST(year AS TEXT) || '-01-01' AND CAST(year AS TEXT) || '-12-31'))

        -- OU garder ceux qui commencent le 31 décembre de l'année précédente et finissent le 1er janvier de l'année cible
        OR (pickup_date = CAST(year - 1 AS TEXT) || '-12-31' AND dropoff_date = CAST(year AS TEXT) || '-01-01')
)

SELECT * EXCLUDE (pickup_date, dropoff_date, year) FROM final_data