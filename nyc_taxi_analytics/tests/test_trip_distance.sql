SELECT *
FROM {{ ref('transform') }}
WHERE trip_distance <= 0
