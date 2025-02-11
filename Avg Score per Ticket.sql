SELECT 
    external_ticket_id, 
    AVG(score) AS average_score
FROM autoqa_ratings
GROUP BY external_ticket_id;