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
        PLAYER_SEASON_PLAYED AS SEASON_NAME
        , SEASON_START_YEAR
        , SEASON_END_YEAR
        , NBA_3PT_ERA
        , NBA_ERA
    FROM RAW
    GROUP BY 
        PLAYER_SEASON_PLAYED
        , SEASON_START_YEAR
        , SEASON_END_YEAR
        , NBA_3PT_ERA
        , NBA_ERA
),

DIM_SEASON AS (
select 
    {{ dbt_utils.generate_surrogate_key(['SEASON_NAME']) }} as SEASON_ID,
    * 
    , TO_TIMESTAMP(CURRENT_TIMESTAMP ) AS LOAD_DATE
    , 'NBA_DB.CORE_NBA_DATA.MT_SEASON_PLAYER_STATS_WITH_DRAFT_NORMALIZED' AS SOURCE_TABLE
from GROUPING_CLAUSE
)

SELECT 
    * 
FROM DIM_SEASON