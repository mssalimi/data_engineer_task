WITH ReviewCounts AS (
    SELECT 
        reviewee_id, 
        COUNT(review_id) AS review_count
    FROM manual_reviews_test
    GROUP BY reviewee_id
)
SELECT 
    m.reviewee_id, 
    AVG(m.score) AS average_review_score
FROM manual_reviews_test m
JOIN ReviewCounts rc ON m.reviewee_id = rc.reviewee_id
WHERE rc.review_count >= 2
GROUP BY m.reviewee_id;
