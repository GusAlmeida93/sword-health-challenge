WITH flag as (
    SELECT 
    id
    ,patient_id
    ,date
    ,MONTHNAME(date) as month
    ,YEAR(date) as year
    ,CASE 
        WHEN scores.satisfaction >= 8 THEN 'Promoter'
        ELSE 'Detractor'
        END as NPS
    FROM scores
),
grouped as (
    SELECT
        year
        ,month
        ,COUNT(CASE WHEN NPS = 'Promoter' THEN 1 END) as Promoters
        ,COUNT(CASE WHEN NPS = 'Detractor' THEN 1 END) as Detractors
        ,COUNT(*) as Total
    FROM flag
    GROUP BY 
        year
        ,month
)
SELECT
    year
    ,month
    ,(Promoters - Detractors) / Total as NPS
FROM grouped