with source as (
    select * 
    from {{ source('STAGING_NBA_DATA', 'STG_REG_SEASON_TOTALS') }}
),

renamed as (
    select
        game_id,
        team_id,
        to_date(game_date) as game_date,  -- Konwersja do DATE
        season_year,
        team_name,
        team_abbreviation,
        matchup,
        wl,
        cast(min as number) as game_length,
        fgm as fg_made,
        fga as fg_attempted,
        fg_pct,
        fg3m as fg3_made,
        fg3a as fg3_attempted,
        fg3_pct,
        ftm as ft_made,
        fta as ft_attempted,
        ft_pct,
        oreb,
        dreb,
        reb,
        ast,
        tov,
        stl,
        blk,
        pf,
        pts,
        plus_minus,
        current_timestamp() as load_date  -- Timestamp ładowania
    from source
)

select * from renamed
