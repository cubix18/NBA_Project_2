{{
  config(
    materialized = 'table',
    tags = ['PLAYER', 'CLEANSE', 'NBA_PROJECT', 'PLAYER_ORIENTED']
  )
}}


WITH CLS_SEASON_PLAYER_FANTASY_2425 AS (
    SELECT 
        *
    FROM {{ ref('CLS_SEASON_PLAYER_FANTASY_2425') }} 
),

CLS_SEASON_PLAYER_DATASET_2425 as (
    SELECT 
    111111111 AS PLAYER_ID
    , PLAYER_FULL_NAME
    , '2024-25' AS PLAYER_SEASON_PLAYED
    , 2024 AS START_OF_SEASON
    , 2025 AS END_OF_SEASON
    , case team_abbreviation
            when 'ATL' then 'Atlanta'
            when 'BOS' then 'Boston'
            when 'BRK' then 'Brooklyn'
            when 'NJN' then 'New Jersey'
            when 'CHO' then 'Charlotte'
            when 'CHH' then 'Charlotte'
            when 'CHI' then 'Chicago'
            when 'CLE' then 'Cleveland'
            when 'DAL' then 'Dallas'
            when 'DEN' then 'Denver'
            when 'DET' then 'Detroit'
            when 'GSW' then 'Golden State'
            when 'GOS' then 'Golden State'
            when 'HOU' then 'Houston'
            when 'SAS' then 'San Antonio'
            when 'IND' THEN 'Indiana'
            when 'LAC' then 'Los Angeles'
            when 'LAL' then 'Los Angeles'
            when 'MEM' then 'Memphis'
            when 'MIA' then 'Miami'
            when 'MIL' then 'Milwaukee'
            when 'MIN' then 'Minnesota'
            when 'NOP' then 'New Orleans'
            when 'NOH' then 'New Orleans'
            when 'NOK' then 'New Orleans'
            when 'NYK' then 'New York'
            when 'NYN' then 'New York'
            when 'ORL' then 'Orlando'
            when 'PHI' then 'Philadelphia'
            when 'PHO' then 'Phoenix'
            when 'POR' then 'Portland'
            when 'SAC' then 'Sacramento'
            when 'SEA' then 'Seattle'
            when 'TOR' then 'Toronto'
            when 'UTA' then 'Utah'
            when 'OKC' then 'Oklahoma City'
            when 'WAS' then 'Washington'
            else 'Unknown'
    end as TEAM_CITY
    , CASE team_abbreviation
        when 'ATL' then 'Atlanta Hawks'
        when 'BOS' then 'Boston Celtics'
        when 'BRK' then 'Brooklyn Nets'
        when 'NJN' then 'New Jersey Nets'
        when 'CHO' then 'Charlotte Hornets'
        when 'CHH' then 'Charlotte Hornets'
        when 'CHI' then 'Chicago Bulls'
        when 'CLE' then 'Cleveland Cavaliers'
        when 'DAL' then 'Dallas Mavericks'
        when 'DEN' then 'Denver Nuggets'
        when 'DET' then 'Detroit Pistons'
        when 'GSW' then 'Golden State Warriors'
        when 'GOS' then 'Golden State Warriors'
        when 'HOU' then 'Houston Rockets'
        when 'SAS' then 'San Antonio Spurs'
        WHEN 'IND' THEN 'Indiana Pacers'
        when 'LAC' then 'Los Angeles Clippers'
        when 'LAL' then 'Los Angeles Lakers'
        when 'MEM' then 'Memphis Grizzlies'
        when 'MIA' then 'Miami Heat'
        when 'MIL' then 'Milwaukee Bucks'
        when 'MIN' then 'Minnesota Timberwolves'
        when 'NOP' then 'New Orleans Pelicans'
        when 'NOH' then 'New Orleans Hornets'
        when 'NOK' then 'New Orleans/Oklahoma City Hornets'
        when 'NYK' then 'New York Knicks'
        when 'NYN' then 'New York Nets'
        when 'ORL' then 'Orlando Magic'
        when 'PHI' then 'Philadelphia 76ers'
        when 'PHO' then 'Phoenix Suns'
        when 'POR' then 'Portland Trail Blazers'
        when 'SAC' then 'Sacramento Kings'
        when 'SEA' then 'Seattle SuperSonics'
        when 'TOR' then 'Toronto Raptors'
        when 'UTA' then 'Utah Jazz'
        when 'OKC' then 'Oklahoma City Thunder'
        when 'WAS' then 'Washington Wizards'
        else 'Unknown'
    end as TEAM_FULL_NAME
    , TEAM_ABBREVIATION
    , PLAYER_AGE
    , PLAYER_GAMES_PLAYED
    , 'Unknown' AS PLAYER_GAMES_STARTED
    , PLAYER_MINUTES_PLAYED
    , PLAYER_FG_MADE, PLAYER_FG_ATTEMPTED, PLAYER_FG_PCT
    , PLAYER_3PT_MADE
    , NULL AS PLAYER_3PT_ATTEMPTD
    , NULL AS PLAYER_3PT_PCT
    , PLAYER_FT_MADE, PLAYER_FT_ATTEMPTED, PLAYER_FT_PCT
    , PLAYER_OFF_REB, PLAYER_DEF_REB, PLAYER_TOT_REB
    , PLAYER_ASSISTS, PLAYER_STEALS, PLAYER_BLOCKS, PLAYER_TOVERS 
    , CAST(0 AS NUMBER) AS PLAYER_FOULS
    , PLAYER_POINTS
    , 1 AS IS_VALID
    , TO_TIMESTAMP(CURRENT_TIMESTAMP ) AS LOAD_DATE
    , 'STAGING_NBA_DATA.CLS_SEASON_PLAYER_FANTASY_2425' AS SOURCE_TABLE
FROM NBA_DB.CLEANSE_NBA_DATA.CLS_SEASON_PLAYER_FANTASY_2425
)

SELECT * FROM CLS_SEASON_PLAYER_DATASET_2425
