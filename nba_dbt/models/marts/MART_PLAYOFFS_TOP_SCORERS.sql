with base as (
    select *
    from {{ ref('CORE_PLAYOFFS_STATS') }}
    where season_year = '2022-23'  -- możesz też użyć Jinja parametru lub makra do dynamicznej filtracji
),

scoring_summary as (
    select
        player_id,
        player_name,
        team_name,
        team_abbrevation,
        season_year,
        count(distinct game_id) as games_played,
        sum(total_pts) as total_points,
        round(sum(total_pts) / count(distinct game_id), 2) as avg_pts_per_game
    from base
    group by player_id, player_name, team_name, team_abbrevation, season_year
)

select *
from scoring_summary
order by avg_pts_per_game desc
limit 1000
