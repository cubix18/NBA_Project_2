with source as (
    select * 
    from {{ source('STAGING_NBA_DATA', 'STG_ALL_TEAM_ARENA_ATTENDANCE') }}
),

CTE AS (
    SELECT 
        TO_DATE(DATE, 'DY, MON DD, YYYY') AS MATCH_DATE
        , REPLACE(SPLIT(DATE, ',')[0], '"','') AS DAY_MATCH_DATE
        , REPLACE("Start (ET)", 'p', '') AS START_DATE
        , "Home/Neutral" AS HOME_TEAM
        ,  PTS_HOME
        , "Visitor/Neutral" AS AWAY_TEAM
        , PTS_VISITOR AS PTS_AWAY
        , COALESCE("Unnamed: 7", 'NORMAL') AS END_OF_MATCH
        , CAST(REPLACE(ATTENDANCE, ',', '.') * 1000 AS NUMBER) AS MATCH_ATTENDANCE
        , INITCAP(ARENA) AS MATCH_ARENA
        , CASE
            WHEN NOTES IS NULL AND TO_DATE(DATE, 'DY, MON DD, YYYY') > '2025-04-18' THEN 'Play-Offs'
            WHEN NOTES IS NULL AND TO_DATE(DATE, 'DY, MON DD, YYYY') <= '2025-04-18' THEN 'Regular Season'
            ELSE NOTES
        END AS MATCH_TYPE
    FROM source
),

CLS_ALL_TEAM_ARENA_ATTENDANCE as (
    SELECT 
        TO_TIMESTAMP(CONCAT(MATCH_DATE, ' ', START_DATE)) AS MATCH_DATE_EST
        , CASE
            WHEN DAY_MATCH_DATE = 'Mon' THEN 'Monday'
            WHEN DAY_MATCH_DATE = 'Tue' THEN 'Tuesday'
            WHEN DAY_MATCH_DATE = 'Wed' THEN 'Wednesday'
            WHEN DAY_MATCH_DATE = 'Thu' THEN 'Thursday'
            WHEN DAY_MATCH_DATE = 'Fri' THEN 'Friday'
            WHEN DAY_MATCH_DATE = 'Sat' THEN 'Saturday'
            WHEN DAY_MATCH_DATE = 'Sun' THEN 'Sunday'
        END AS DAY_OF_MATCH_DATE
        , HOME_TEAM AS TEAM_NAME_HOME
        , PTS_HOME AS TEAM_PTS_HOME
        , AWAY_TEAM AS TEAM_NAME_AWAY
        , PTS_AWAY AS TEAM_PTS_AWAY
        , END_OF_MATCH AS MATCH_TYPE_OF_END
        , MATCH_ATTENDANCE
        , MATCH_ARENA 
        , MATCH_TYPE 
        , 1 AS IS_VALID
        , TO_TIMESTAMP(CURRENT_TIMESTAMP ) AS LOAD_DATE
        , 'STAGING_NBA_DATA.STG_ALL_TEAM_ARENA_ATTENDANCE' AS SOURCE_TABLE
    FROM CTE
)

select * from CLS_ALL_TEAM_ARENA_ATTENDANCE