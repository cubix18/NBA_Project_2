with source as (
    select * 
    from {{ source('STAGING_NBA_DATA', 'STG_DRAFT_PLAYER_HISTORY') }}
),

CTE AS (
    SELECT
    *
    , ROW_NUMBER() OVER (PARTITION BY PERSON_ID ORDER BY SEASON DESC) AS RN
    FROM source
),

CLS_DRAFT_PLAYER_HISTORY as (
    SELECT 
        PLAYER_NAME AS PLAYER_FULL_NAME
        , SEASON AS DRAFT_YEAR
        , ROUND_NUMBER
        , ROUND_PICK
        , OVERALL_PICK
        , DRAFT_TYPE
        , CONCAT(TEAM_CITY, ' ', TEAM_NAME) AS TEAM_NAME
        , TEAM_ABBREVIATION
        , TEAM_NAME AS TEAM_NICKNAME
        , TEAM_CITY
        , ORGANIZATION AS TEAM_ORGANIZTION
        , ORGANIZATION_TYPE AS TEAM_ORGANIZTION_TYPE
        , CASE 
            WHEN RN = 1 THEN TRUE
            WHEN RN > 1 THEN FALSE
            ELSE NULL
        END AS IS_VALID
        , TO_TIMESTAMP(CURRENT_TIMESTAMP ) AS LOAD_DATE
        , 'STAGING_NBA_DATA.STG_DRAFT_PLAYER_HISTORY' AS SOURCE_TABLE
    FROM CTE
)

select * from CLS_DRAFT_PLAYER_HISTORY