{{
  config(
    materialized = 'table',
    tags = ['PLAYER', 'CLEANSE', 'NBA_PROJECT', 'PLAYER_ORIENTED']
  )
}}


WITH STG_SEASON_PLAYER_DATASET AS (
    SELECT 
        *
    FROM {{ source('STAGING_NBA_DATA', 'STG_SEASON_PLAYER_DATASET') }}
),

CLS_SEASON_PLAYER_DATASET_2425 AS (
    SELECT 
        *
    FROM {{ ref('CLS_SEASON_PLAYER_DATASET_2425') }} 
),

CLS_SEASON_PLAYER_DATASET as (
    SELECT 
        PLAYER_ID
        , INITCAP({{ normalize_name('NAME') }}) AS PLAYER_FULL_NAME
        , SEASON_ID AS PLAYER_SEASON_PLAYED
        , CAST(REPLACE(SPLIT(SEASON_ID, '-')[0], '"', '' ) AS NUMBER) AS START_OF_SEASON
        , CASE
            WHEN REPLACE(SPLIT(SEASON_ID, '-')[1], '"', '' ) > 40 THEN CAST(CONCAT('19','',REPLACE(SPLIT(SEASON_ID, '-')[1], '"', '' )) AS NUMBER)
            ELSE CAST(CONCAT('20','',REPLACE(SPLIT(SEASON_ID, '-')[1], '"', '' )) AS NUMBER)
        END AS END_OF_SEASON
        , case team_abbreviation
            when 'ATL' then 'Atlanta'
            when 'BOS' then 'Boston'
            when 'BKN' then 'Brooklyn'
            when 'NJN' then 'New Jersey'
            when 'CHA' then 'Charlotte'
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
            when 'PHX' then 'Phoenix'
            when 'POR' then 'Portland'
            when 'SAC' then 'Sacramento'
            when 'SEA' then 'Seattle'
            when 'TOR' then 'Toronto'
            when 'UTA' then 'Utah'
            when 'OKC' then 'Oklahoma City'
            when 'WAS' then 'Washington'
            -- Historical
            when 'FTW' then 'Ft. Wayne'
            when 'MNL' then 'Minneapolis'
            when 'ROC' then 'Rochester'
            when 'CIN' then 'Cincinnati'
            when 'STL' then 'St. Louis'
            when 'BUF' then 'Buffalo'
            when 'SDC' then 'San Diego'
            when 'SDR' then 'San Diego'
            when 'VAN' then 'Vancouver'
            when 'SFW' then 'San Francisco'
            when 'PHW' then 'Philadelphia'
            when 'BAL' then 'Baltimore'
            when 'BLT' then 'Baltimore'
            when 'CAP' then 'Capital'
            when 'CHZ' then 'Chicago'
            when 'CHS' then 'Chicago'
            when 'CHP' then 'Chicago'
            when 'INO' then 'Indianapolis'
            when 'JET' then 'Indianapolis'
            when 'KCK' then 'Kansas City'
            when 'NOJ' then 'New Orleans'
            when 'WAT' then 'Waterloo'
            when 'PRO' then 'Providence'
            when 'SHE' then 'Sheboygan'
            when 'BOM' then 'St. Louis'
            when 'DEF' then 'Detroit'
            when 'AND' then 'Anderson'
            when 'PIT' then 'Pittsburgh'
            when 'SYR' then 'Syracuse'
            when 'HUS' then 'Toronto'
            when 'TCB' then 'Tri-Cities'
            when 'CLR' then 'Cleveland'
            else 'Unknown'
    end as TEAM_CITY
    -- full_name
    , case team_abbreviation
        when 'ATL' then 'Atlanta Hawks'
        when 'BOS' then 'Boston Celtics'
        when 'BKN' then 'Brooklyn Nets'
        when 'NJN' then 'New Jersey Nets'
        when 'CHA' then 'Charlotte Hornets'
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
        when 'PHX' then 'Phoenix Suns'
        when 'POR' then 'Portland Trail Blazers'
        when 'SAC' then 'Sacramento Kings'
        when 'SEA' then 'Seattle SuperSonics'
        when 'TOR' then 'Toronto Raptors'
        when 'UTA' then 'Utah Jazz'
        when 'OKC' then 'Oklahoma City Thunder'
        when 'WAS' then 'Washington Wizards'
        -- historyczne
        when 'FTW' then 'Ft. Wayne Zollner Pistons'
        when 'MNL' then 'Minneapolis Lakers'
        when 'ROC' then 'Rochester Royals'
        when 'CIN' then 'Cincinnati Royals'
        when 'STL' then 'St. Louis Hawks'
        when 'BUF' then 'Buffalo Braves'
        when 'SDC' then 'San Diego Clippers'
        when 'SDR' then 'San Diego Rockets'
        when 'VAN' then 'Vancouver Grizzlies'
        when 'SFW' then 'San Francisco Warriors'
        when 'PHW' then 'Philadelphia Warriors'
        when 'BAL' then 'Baltimore Bullets'
        when 'BLT' then 'Baltimore Bullets'
        when 'CAP' then 'Capital Bullets'
        when 'CHZ' then 'Chicago Zephyrs'
        when 'CHS' then 'Chicago Stags'
        when 'CHP' then 'Chicago Packers'
        when 'INO' then 'Indianapolis Olympians'
        when 'JET' then 'Indianapolis Jets'
        when 'KCK' then 'Kansas City Kings'
        when 'NOJ' then 'New Orleans Jazz'
        when 'WAT' then 'Waterloo Hawks'
        when 'PRO' then 'Providence Steamrollers'
        when 'SHE' then 'Sheboygan Redskins'
        when 'BOM' then 'St. Louis Bombers'
        when 'DEF' then 'Detroit Falcons'
        when 'AND' then 'Anderson Packers'
        when 'PIT' then 'Pittsburgh Ironmen'
        when 'SYR' then 'Syracuse Nationals'
        when 'HUS' then 'Toronto Huskies'
        when 'TCB' then 'Tri-Cities Blackhawks'
        when 'CLR' then 'Cleveland Rebels'
        else 'Unknown'
    end as TEAM_FULL_NAME
        , TEAM_ABBREVIATION
        , PLAYER_AGE
        , GP AS PLAYER_GAMES_PLAYED
        , GS AS PLAYER_GAMES_STARTED
        , CASE
            WHEN MIN = 'None' THEN 0
            ELSE CAST(MIN AS NUMBER)
        END AS PLAYER_MINUTES_PLAYED
        , FGM AS PLAYER_FG_MADE
        , FGA AS PLAYER_FG_ATTEMPTED
        , CASE
            WHEN FG_PCT = 'None' THEN 0
            ELSE CAST(FG_PCT AS FLOAT)
        END AS PLAYER_FG_PCT
        , CASE
            WHEN FG3M = 'None' THEN 0
            ELSE CAST(FG3M AS NUMBER)
        END AS PLAYER_3PT_MADE
        , CASE
            WHEN FG3A = 'None' THEN 0
            ELSE CAST(FG3A AS NUMBER)
        END AS PLAYER_3PT_ATTEMPTED
        , CASE
            WHEN FG3_PCT = 'None' THEN 0
            ELSE CAST(FG3_PCT AS FLOAT)
        END AS PLAYER_3PT_PCT
        , FTM AS PLAYER_FT_MADE
        , FTA AS PLAYER_FT_ATTEMMPTED
        , CASE
            WHEN FT_PCT = 'None' THEN 0
            ELSE CAST(FT_PCT AS FLOAT)
        END AS PLAYER_FT_PCT
        , CASE
            WHEN OREB = 'None' THEN 0
            ELSE CAST(OREB AS FLOAT)
        END AS PLAYER_OFF_REB
        , CASE
            WHEN DREB = 'None' THEN 0
            ELSE CAST(DREB AS FLOAT)
        END AS PLAYER_DEF_REB
        , CASE
            WHEN REB = 'None' THEN 0
            ELSE CAST(REB AS FLOAT)
        END AS PLAYER_TOT_REB
        , AST AS PLAYER_ASSISTS
        , CASE
            WHEN STL = 'None' THEN 0
            ELSE CAST(STL AS FLOAT)
        END AS PLAYER_STEALS
        , CASE
            WHEN BLK = 'None' THEN 0
            ELSE CAST(BLK AS FLOAT)
        END AS PLAYER_BLOCKS
        , CASE
            WHEN TOV = 'None' THEN 0
            ELSE CAST(TOV AS FLOAT)
        END AS PLAYER_TOVERS
        , PF AS PLAYER_FOULS
        , PTS AS PLAYER_POINTS
        , 1 AS IS_VALID
        , TO_TIMESTAMP(CURRENT_TIMESTAMP ) AS LOAD_DATE
        , 'STAGING_NBA_DATA.STG_SEASON_PLAYER_DATASET' AS SOURCE_TABLE
    FROM STG_SEASON_PLAYER_DATASET
),

UNIONING AS (
SELECT * FROM CLS_SEASON_PLAYER_DATASET
UNION ALL
SELECT * FROM CLS_SEASON_PLAYER_DATASET_2425
)

SELECT * FROM UNIONING
