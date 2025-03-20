SELECT *
FROM {{ ref('transform') }}
WHERE trip_duration_minutes <= 0
