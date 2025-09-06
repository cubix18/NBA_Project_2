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
from GROUPING_CLAUSE
)

SELECT 
    * 
FROM DIM_SEASON