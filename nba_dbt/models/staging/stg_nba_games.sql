SELECT
    id AS game_id,
    TRY_TO_DATE(date) AS game_date,
    home_team_name,
    visitor_team_name,
    TRY_CAST(home_team_score AS INTEGER) AS home_score,
    TRY_CAST(visitor_team_score AS INTEGER) AS visitor_score
FROM {{ source('raw', 'nba_games_full') }}