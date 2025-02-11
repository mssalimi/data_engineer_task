
WITH NormalizedRatings AS (
    SELECT 
        category_id,
        SUM(CASE 
                WHEN rating IS NOT NULL AND rating != 42 
                THEN (rating * 100.0 / rating_max) * weight 
                ELSE 0 
            END) AS weighted_score,
        SUM(CASE 
                WHEN rating IS NOT NULL AND rating != 42 
                THEN weight 
                ELSE 0 
            END) AS total_weight
    FROM `oceanic-abacus-450215-c2.manual_test1.Manual_rating` 
    GROUP BY category_id
)
SELECT 
    category_id,
    CASE 
        WHEN total_weight > 0 
        THEN ROUND(weighted_score / total_weight, 2) 
        ELSE 0 
    END AS category_score_percentage
FROM NormalizedRatings
ORDER BY category_score_percentage DESC;
