{{ config(
    materialized='external',
    location='output/trips_{{ var("year") }}_{{ var("month") }}_transformed.parquet',
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

        -- Variables dynamiques pour l'année et le mois
        CAST({{ var('year') }} AS TEXT) AS year,
        LPAD(CAST({{ var('month') }} AS TEXT), 2, '0') AS month  -- Ajout du zéro si nécessaire
    FROM transformed_data
    WHERE 
        -- Garder uniquement les trajets qui commencent et finissent dans le mois et l'année cible
        pickup_date BETWEEN '{{ var("year") }}-{{ var("month") }}-01' AND '{{ var("year") }}-{{ var("month") }}-31'
        AND dropoff_date BETWEEN '{{ var("year") }}-{{ var("month") }}-01' AND '{{ var("year") }}-{{ var("month") }}-31'
)

SELECT * EXCLUDE (pickup_date, dropoff_date, year, month) FROM final_data