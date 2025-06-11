SELECT
    HOME_TEAM
    , SUM(HOME_SCORE) AS total_home_score
    , SUM(VISITOR_SCORE) AS total_visitor_score
FROM {{ source('raw', 'nba_games') }}
GROUP BY HOME_TEAM
