-- models/core/CORE_PLAYOFFS_STATS_TEAM.sql

with base as (
    select * from {{ ref('CLS_PLAYOFFS_PLAYER_STATS') }}
),

CORE_PLAYOFFS_STATS as (
    select
        PLAYER_ID
        , GAME_ID
        , TEAM_ID
        , TEAM_NAME
        , TEAM_ABBREVATION
        , TEAM_CITY
        , SEASON_YEAR
        , GAME_DATE
        , PLAYER_NAME
        , POSITION
        , COMMENT
        , SUM(MINUTES) AS TOTAL_MINUTES
        , SUM(SECONDS) AS TOTAL_SECONDS
        , sum(fg_made) as total_fg_made
        , sum(fg_attempted) as total_fg_attempted
        , avg(fg_pct) as avg_fg_pct
        , sum(fg3_made) as total_fg3_made
        , sum(fg3_attempted) as total_fg3_attempted
        , avg(fg3_pct) as avg_fg3_pct
        , sum(ft_made) as total_ft_made
        , sum(ft_attempted) as total_ft_attempted
        , avg(ft_pct) as avg_ft_pct
        , sum(oreb) as total_oreb
        , sum(dreb) as total_dreb
        , SUM(REB_TOTAL) AS TOTAL_ALL_REB
        , sum(ast) as total_ast
        , sum(stl) as total_stl
        , sum(blk) as total_blk
        , sum(tov) as total_tov
        , sum(FOULS) as total_pf
        , sum(pts) as total_pts
        , sum(PLUS_MINUS) as total_plus_minus
    from base
    group by PLAYER_ID, GAME_ID, TEAM_ID, TEAM_NAME, TEAM_ABBREVATION, TEAM_CITY, SEASON_YEAR, GAME_DATE, PLAYER_NAME, POSITION, COMMENT
)

select * from CORE_PLAYOFFS_STATS
