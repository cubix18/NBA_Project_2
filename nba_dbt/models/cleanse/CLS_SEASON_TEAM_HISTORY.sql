with source as (
    select * 
    from {{ source('STAGING_NBA_DATA', 'STG_SEASON_TEAM_HISTORY') }}
),

CLS_SEASON_TEAM_HISTORY as (
    SELECT 
        TEAM_ID
        , CONCAT(CITY, ' ', NICKNAME) AS TEAM_NAME
        , CITY AS TEAM_CITY
        , NICKNAME AS TEAM_NICKNAME
        , YEAR_FOUNDED AS TEAM_FOUND_YEAR
        , YEAR_ACTIVE_TILL AS TEAM_TILL_YEAR
        , 1 AS IS_VALID
        , TO_TIMESTAMP(CURRENT_TIMESTAMP ) AS LOAD_DATE
        , 'STAGING_NBA_DATA.STG_SEASON_TEAM_HISTORY' AS SOURCE_TABLE
    from source
)

select * from CLS_SEASON_TEAM_HISTORY