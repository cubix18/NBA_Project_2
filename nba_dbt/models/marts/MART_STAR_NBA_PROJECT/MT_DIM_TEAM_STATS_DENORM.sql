{{
  config(
    materialized = "table",
    tags = ["STAR_SCHEMA"]
  )
}}

WITH RAW AS (

    SELECT *
    FROM {{ ref('MT_SEASON_PLAYER_STATS_WITH_DRAFT_NORMALIZED') }}

),

GROUPING_CLAUSE as (

    SELECT
        TEAM_NAME
        , TEAM_ABBREVIATION
        , TEAM_CITY
        , TEAM_CONFERENCE
        , TEAM_DIVISION
        , TEAM_STATUS  
    FROM RAW
    GROUP BY 
        TEAM_NAME
        , TEAM_ABBREVIATION
        , TEAM_CITY
        , TEAM_CONFERENCE
        , TEAM_DIVISION
        , TEAM_STATUS
        
),

DIM_TEAM AS (
SELECT 
    {{ dbt_utils.generate_surrogate_key(['TEAM_NAME']) }} as TEAM_ID,
    * 
    , TO_TIMESTAMP(CURRENT_TIMESTAMP ) AS LOAD_DATE
    , 'NBA_DB.CORE_NBA_DATA.MT_SEASON_PLAYER_STATS_WITH_DRAFT_NORMALIZED' AS SOURCE_TABLE
FROM GROUPING_CLAUSE
)

SELECT 
    * 
FROM DIM_TEAM