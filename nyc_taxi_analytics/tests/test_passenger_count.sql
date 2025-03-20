SELECT *
FROM {{ ref('transform') }}
WHERE 
    passenger_count <= 0  -- Vérifie que toutes les valeurs sont strictement positives
    OR passenger_count != CAST(passenger_count AS BIGINT)  -- Vérifie que les valeurs sont bien des entiers
