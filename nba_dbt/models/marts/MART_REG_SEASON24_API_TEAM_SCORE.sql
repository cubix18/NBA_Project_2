
SELECT
    home_team_name,
    COUNT(*) AS home_games_count,
    SUM(home_score) AS total_home_score,
    SUM(visitor_score) AS total_visitor_score,
    AVG(home_score) AS avg_home_score,
    AVG(visitor_score) AS avg_visitor_score
FROM {{ ref('CLS_REG_SEASON24_API') }}
GROUP BY home_team_name