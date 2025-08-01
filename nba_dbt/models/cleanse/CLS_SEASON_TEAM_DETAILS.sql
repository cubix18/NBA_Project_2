with source as (
    select * 
    from {{ source('STAGING_NBA_DATA', 'STG_SEASON_TEAM_DETAILS') }}
),

CLS_SEASON_TEAM_DETAILS as (
    SELECT 
        CONCAT(CITY, ' ', NICKNAME) AS TEAM_NAME
        , ABBREVIATION AS TEAM_ABBREVIATION
        , NICKNAME AS TEAM_NICKNAME
        , CITY AS TEAM_CITY
        , CAST(YEARFOUNDED AS NUMBER) AS TEAM_FOUND_YEAR
        , ARENA AS TEAM_ARENA_NAME
        , CAST(ARENACAPACITY AS NUMBER) AS TEAM_ARENA_CAPACITY
        , OWNER AS TEAM_OWNER_NAME
        , GENERALMANAGER AS TEAM_GENERAL_MANAGER_NAME
        , HEADCOACH AS TEAM_HEADCOACH_NAME
        , DLEAGUEAFFILIATION AS TEAM_DLEAGUE_AFFILIATION_NAME
        , 1 AS IS_VALID
        , TO_TIMESTAMP(CURRENT_TIMESTAMP ) AS LOAD_DATE
        , 'STAGING_NBA_DATA.STG_SEASON_TEAM_DETAILS' AS SOURCE_TABLE
    from source
)

select * from CLS_SEASON_TEAM_DETAILS