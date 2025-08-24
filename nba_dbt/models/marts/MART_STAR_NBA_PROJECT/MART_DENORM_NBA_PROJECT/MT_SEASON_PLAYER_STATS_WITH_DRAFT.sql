WITH CTA AS (
    SELECT 
        DAF.PLAYER_FULL_NAME
        , DAF.PLAYER_SEASON_PLAYED
        , CASE
            WHEN AWARDS.AWARD = 'MVP' THEN 1
            ELSE FALSE
        END AS IS_MVP
        , CASE
            WHEN AWARDS.AWARD = 'Rookie of the Year' THEN 1
            ELSE FALSE
        END AS IS_ROTY
        , CASE
            WHEN AWARDS.AWARD = 'Defensive Player of the Year' THEN 1
            ELSE FALSE
        END AS IS_DPOY
        , CASE
            WHEN AWARDS.AWARD = 'Sixth Man of the Year' THEN 1
            ELSE FALSE
        END AS IS_SIXTH_MAN
        , CASE
            WHEN AWARDS.AWARD = 'Most Improved Player' THEN 1
            ELSE FALSE
        END AS IS_MIP
    FROM {{ ref('CO_SEASON_PLAYER_STATS_WITH_DRAFT_AF') }} DAF
    LEFT JOIN {{ ref('CLS_SEASON_PLAYER_AWARDS') }} AWARDS
    ON DAF.PLAYER_FULL_NAME = AWARDS.PLAYER_FULL_NAME
    AND DAF.PLAYER_SEASON_PLAYED = AWARDS.PLAYER_SEASON_PLAYED
),


CTB AS (
    SELECT PLAYER_FULL_NAME AS NAME, PLAYER_SEASON_PLAYED AS SEASON, MAX(IS_MVP) AS IS_MVP, MAX(IS_ROTY) AS IS_ROTY , MAX(IS_DPOY) AS IS_DPOY , MAX(IS_SIXTH_MAN) AS IS_SIXTH_MAN , MAX(IS_MIP) AS IS_MIP
    FROM CTA
    GROUP BY PLAYER_FULL_NAME, PLAYER_SEASON_PLAYED
),


CTE AS (
SELECT 
    PLAYER_FULL_NAME
    , PLAYER_BIRTH_DATE
    , PLAYER_SEASON_PLAYED
    , PLAYER_SCHOOL
    , PLAYER_COUNTRY
    , PLAYER_PREVIOUS_TEAM
    , PLAYER_PREVIOUS_TEAM_COUNTRY
    , PLAYER_DRAFT_YEAR
    , PLAYER_DRAFT_ROUND
    , PLAYER_DRAFT_NUMBER
    , PLAYER_IS_GREATEST_75
    , SEASON_START_YEAR
    , SEASON_END_YEAR
    , PLAYER_HEIGHT_IN_CM
    , PLAYER_WEIGHT_IN_KG
    , PLAYER_SEASON_EXPERIENCE
    , PLAYER_MAIN_POSITION
    , PLAYER_SECOND_POSITION
    , PLAYER_ROSTER_STATUS
    , PLAYED_IN_DLEAGUE
    , AWARDS.IS_MVP
    , AWARDS.IS_ROTY
    , AWARDS.IS_DPOY
    , AWARDS.IS_SIXTH_MAN
    , AWARDS.IS_MIP
    , CASE
        WHEN SEASON_START_YEAR >=  1979 THEN 'PRE-3PT'
        ELSE 'POST-3PT-ERA'
    END AS NBA_3PT_ERA
    , CASE
        WHEN SEASON_START_YEAR BETWEEN 1946 AND 1979 THEN 'Pioneer / Russell-Wilt Era'
        WHEN SEASON_START_YEAR BETWEEN 1980 AND 1991 THEN 'Magic vs Bird Era'
        WHEN SEASON_START_YEAR BETWEEN 1991 AND 2002 THEN 'Jordan Era'
        WHEN SEASON_START_YEAR BETWEEN 2002 AND 2011 THEN 'Kobe Era'
        WHEN SEASON_START_YEAR BETWEEN 2011 AND 2022 THEN 'LeBron Era'
        WHEN SEASON_START_YEAR >= 2023 THEN 'Modern Era'
        ELSE 'Unknown'
    END AS NBA_ERA
    , TEAM_NAME
    , TEAM_ABBREVIATION
    , TEAM_CITY
    , CASE 
        WHEN TEAM_NAME IN ('Toronto Raptors','New York Knicks','Brooklyn Nets','Boston Celtics','Philadelphia 76ers')
            THEN 'EAST'
        WHEN TEAM_NAME IN ('Chicago Bulls','Cleveland Cavaliers','Detroit Pistons','Indiana Pacers','Milwaukee Bucks')
            THEN 'EAST'
        WHEN TEAM_NAME IN ('Atlanta Hawks','Miami Heat','Orlando Magic','Charlotte Hornets','Washington Wizards')
            THEN 'EAST'
        WHEN TEAM_NAME IN ('Denver Nuggets','Minnesota Timberwolves','Oklahoma City Thunder','Portland Trail Blazers','Utah Jazz')
            THEN 'WEST'
        WHEN TEAM_NAME IN ('Golden State Warriors','Los Angeles Lakers','Los Angeles Clippers','La Clippers','Phoenix Suns','Sacramento Kings')
            THEN 'WEST'
        WHEN TEAM_NAME IN ('Dallas Mavericks','Houston Rockets','Memphis Grizzlies','New Orleans Pelicans','San Antonio Spurs')
            THEN 'WEST'
        ELSE 'Historical/Unknown'
    END AS TEAM_CONFERENCE
    , CASE
        WHEN TEAM_NAME IN ('Toronto Raptors','New York Knicks','Brooklyn Nets','Boston Celtics','Philadelphia 76ers')
            THEN 'Atlantic'
        WHEN TEAM_NAME IN ('Chicago Bulls','Cleveland Cavaliers','Detroit Pistons','Indiana Pacers','Milwaukee Bucks')
            THEN 'Central'
        WHEN TEAM_NAME IN ('Atlanta Hawks','Miami Heat','Orlando Magic','Charlotte Hornets','Washington Wizards')
            THEN 'Southeast'
        WHEN TEAM_NAME IN ('Denver Nuggets','Minnesota Timberwolves','Oklahoma City Thunder','Portland Trail Blazers','Utah Jazz')
            THEN 'Northwest'
        WHEN TEAM_NAME IN ('Golden State Warriors','Los Angeles Lakers','Los Angeles Clippers','La Clippers','Phoenix Suns','Sacramento Kings')
            THEN 'Pacific'
        WHEN TEAM_NAME IN ('Dallas Mavericks','Houston Rockets','Memphis Grizzlies','New Orleans Pelicans','San Antonio Spurs')
            THEN 'Southwest'
        ELSE 'Historical/Unknown'
    END AS TEAM_DIVISION
    , CASE 
        WHEN TEAM_NAME IN (
            'Toronto Raptors','New York Knicks','Brooklyn Nets','Boston Celtics','Philadelphia 76ers',
            'Chicago Bulls','Cleveland Cavaliers','Detroit Pistons','Indiana Pacers','Milwaukee Bucks',
            'Atlanta Hawks','Miami Heat','Orlando Magic','Charlotte Hornets','Washington Wizards',
            'Denver Nuggets','Minnesota Timberwolves','Oklahoma City Thunder','Portland Trail Blazers','Utah Jazz',
            'Golden State Warriors','Los Angeles Lakers','Los Angeles Clippers','La Clippers','Phoenix Suns','Sacramento Kings',
            'Dallas Mavericks','Houston Rockets','Memphis Grizzlies','New Orleans Pelicans','San Antonio Spurs'
        ) THEN 'Active'
        WHEN TEAM_NAME = 'Unknown Unknown' THEN 'Unknown'
        ELSE 'Historical'
    END AS TEAM_STATUS
    , TEAM_FOUND_YEAR
    , TEAM_TILL_YEAR
    , PLAYER_AGE
    , PLAYER_GAMES_PLAYED
    , CASE
        WHEN PLAYER_GAMES_STARTED = 'None' THEN 0
        ELSE CAST(PLAYER_GAMES_STARTED AS NUMBER)
    END AS PLAYER_GAMES_STARTED
    , CASE
        WHEN PLAYER_MINUTES_PLAYED = 'None' THEN 0
        ELSE CAST(PLAYER_MINUTES_PLAYED AS NUMBER)
    END AS PLAYER_MINUTES_PLAYED
    , PLAYER_FG_MADE
    , PLAYER_FG_ATTEMPTED
    , CASE
        WHEN PLAYER_FG_PCT = 'None' THEN 0
        ELSE CAST(PLAYER_FG_PCT AS NUMBER)
    END AS PLAYER_FG_PCT
    , CASE
        WHEN PLAYER_3PT_MADE = 'None' THEN NULL
        ELSE CAST(PLAYER_3PT_MADE AS NUMBER)
    END AS PLAYER_3PT_MADE
    , CASE
        WHEN PLAYER_3PT_ATTEMPTED = 'None' THEN NULL
        ELSE CAST(PLAYER_3PT_ATTEMPTED AS NUMBER)
    END AS PLAYER_3PT_ATTEMPTED
    , CASE
        WHEN PLAYER_3PT_PCT = 'None' THEN NULL
        ELSE CAST(PLAYER_3PT_PCT AS NUMBER)
    END AS PLAYER_3PT_PCT
    , PLAYER_FT_MADE
    , PLAYER_FT_ATTEMMPTED
    , CASE
        WHEN PLAYER_FT_PCT = 'None' THEN 0
        ELSE CAST(PLAYER_FT_PCT AS NUMBER)
    END AS PLAYER_FT_PCT
    , CASE
        WHEN PLAYER_OFF_REB = 'None' THEN 0
        ELSE CAST(PLAYER_OFF_REB AS NUMBER)
    END AS PLAYER_OFF_REB
    , CASE
        WHEN PLAYER_DEF_REB = 'None' THEN 0
        ELSE CAST(PLAYER_DEF_REB AS NUMBER)
    END AS PLAYER_DEF_REB
    , CASE
        WHEN PLAYER_TOT_REB = 'None' THEN 0
        ELSE CAST(PLAYER_TOT_REB AS NUMBER)
    END AS PLAYER_TOT_REB
    , PLAYER_ASSISTS
    , CASE
        WHEN PLAYER_STEALS = 'None' THEN 0
        ELSE CAST(PLAYER_STEALS AS NUMBER)
    END AS PLAYER_STEALS
    , CASE
        WHEN PLAYER_BLOCKS = 'None' THEN 0
        ELSE CAST(PLAYER_BLOCKS AS NUMBER)
    END AS PLAYER_BLOCKS
    , CASE
        WHEN PLAYER_TOVERS = 'None' THEN 0
        ELSE CAST(PLAYER_TOVERS AS NUMBER)
    END AS PLAYER_TOVERS
    , PLAYER_FOULS
    , PLAYER_POINTS
FROM {{ ref('CO_SEASON_PLAYER_STATS_WITH_DRAFT_AF') }} DAF
    LEFT JOIN CTB AWARDS
    ON DAF.PLAYER_FULL_NAME = AWARDS.NAME
    AND DAF.PLAYER_SEASON_PLAYED = AWARDS.SEASON
),

CTF AS (
SELECT 
    *
    , ROUND((PLAYER_FG_MADE ) / PLAYER_GAMES_PLAYED,2) AS PLAYER_FG_MADE_AVG
    , ROUND((PLAYER_FG_ATTEMPTED ) / PLAYER_GAMES_PLAYED ,2) AS PLAYER_FG_ATTEMPTED_AVG
    , ROUND((PLAYER_FG_PCT ) / PLAYER_GAMES_PLAYED ,2) AS PLAYER_FG_PCT_AVG
    , ROUND((PLAYER_3PT_MADE ) / PLAYER_GAMES_PLAYED ,2) AS PLAYER_3PT_MADE_AVG
    , ROUND((PLAYER_3PT_ATTEMPTED ) / PLAYER_GAMES_PLAYED ,2) AS PLAYER_3PT_ATTEMPTEDE_AVG
    , ROUND((PLAYER_3PT_PCT ) / PLAYER_GAMES_PLAYED ,2) AS PLAYER_3PT_PCT_AVG
    , ROUND((PLAYER_FT_MADE ) / PLAYER_GAMES_PLAYED ,2) AS PLAYER_FT_MADE_AVG
    , ROUND((PLAYER_FT_ATTEMMPTED) / PLAYER_GAMES_PLAYED ,2) AS PLAYER_FT_ATTEMMPTED_AVG
    , ROUND((PLAYER_FT_PCT ) / PLAYER_GAMES_PLAYED ,2) AS PLAYER_FT_PCT_AVG
    , ROUND((PLAYER_OFF_REB ) / PLAYER_GAMES_PLAYED ,2) AS PLAYER_OFF_REB_AVG
    , ROUND((PLAYER_DEF_REB ) / PLAYER_GAMES_PLAYED ,2) AS PLAYER_DEF_REB_AVG
    , ROUND((PLAYER_TOT_REB ) / PLAYER_GAMES_PLAYED ,2) AS PLAYER_TOT_REB_AVG
    , ROUND((PLAYER_ASSISTS ) / PLAYER_GAMES_PLAYED ,2) AS PLAYER_ASSISTS_AVG
    , ROUND((PLAYER_STEALS ) / PLAYER_GAMES_PLAYED ,2) AS PLAYER_STEALS_AVG
    , ROUND((PLAYER_BLOCKS ) / PLAYER_GAMES_PLAYED ,2) AS PLAYER_BLOCKS_AVG
    , ROUND((PLAYER_TOVERS ) / PLAYER_GAMES_PLAYED ,2) AS PLAYER_TOVERS_AVG
    , ROUND((PLAYER_FOULS ) / PLAYER_GAMES_PLAYED ,2) AS PLAYER_FOULS_AVG
    , ROUND((PLAYER_POINTS ) / PLAYER_GAMES_PLAYED ,2) AS PLAYER_POINTS_AVG
    -- True Shooting %
    , case 
        when (player_fg_attempted + 0.44 * player_ft_attemmpted) > 0 
        then ROUND(player_points / (2 * (player_fg_attempted + 0.44 * player_ft_attemmpted)) ,4)
    end as ts_pct
    -- Effective FG %
    , case 
        when player_fg_attempted > 0 
        then ROUND((player_fg_made + 0.5 * player_3pt_made) / player_fg_attempted ,4)
    end as efg_pct
    -- Turnover %
    , case 
        when (player_fg_attempted + 0.44 * player_ft_attemmpted + player_tovers) > 0
        then ROUND(player_tovers / (player_fg_attempted + 0.44 * player_ft_attemmpted + player_tovers),4)
    end as tov_pct
    -- Free Throw Rate (FTA per FGA)
    , case 
        when player_fg_attempted > 0 
        then ROUND(player_ft_attemmpted / player_fg_attempted,4)
    end as ftr
    -- 3 Point Attempt Rate (3PA per FGA)
    , case 
        when player_fg_attempted > 0 
        then ROUND(player_3pt_attempted / player_fg_attempted,4)
    end as threepar
    -- Rebound Splits
    , case 
        when player_tot_reb > 0 then ROUND(player_off_reb / player_tot_reb, 4) end as orb_pct_est
    , case 
        when player_tot_reb > 0 then ROUND(player_def_reb / player_tot_reb, 4) end as drb_pct_est
    -- Assist-to-Turnover Ratio
    , case 
        when player_tovers > 0 then ROUND(player_assists / player_tovers, 4)
    end as ast_tov_ratio
    , TO_TIMESTAMP(CURRENT_TIMESTAMP ) AS LOAD_DATE
    , 'NBA_DB.CORE_NBA_DATA.CO_SEASON_PLAYER_STATS_WITH_DRAFT_AF' AS SOURCE_TABLE_1
FROM CTE
WHERE PLAYER_GAMES_PLAYED > 0)

SELECT * FROM CTF
