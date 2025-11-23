{{
  config(
    materialized = 'table',
    tags = ['PLAYER', 'CLEANSE', 'NBA_PROJECT', 'PLAYER_ORIENTED']
  )
}}

WITH SOURCE AS (
    SELECT
        *
    FROM {{ source('STAGING_NBA_DATA', 'STG_SEASON_PLAYER_AWARDS') }}
),

CLS_SEASON_PLAYER_AWARDS as (
    SELECT 
        TRIM(SEASON) AS PLAYER_SEASON_PLAYED
        , CASE
            WHEN PLAYER LIKE '%Nikola Jok%' THEN 'Nikola Jokic'
            WHEN PLAYER LIKE '%Manu Gin%' THEN 'Manu Ginobili'
            WHEN PLAYER LIKE '%Toni Kuko%' THEN 'Toni Kukoc'
            WHEN PLAYER LIKE '%Jason Kidd (Tie)%' THEN 'Jason Kidd'
            WHEN PLAYER LIKE '%Steve Francis (Tie)%' THEN 'Steve Francis'
            WHEN PLAYER LIKE '%Goran Drag%' THEN 'Goran Dragic'
            WHEN PLAYER LIKE '%Luka Don%' THEN 'Luka Doncic'
            WHEN PLAYER LIKE '%Gheorghe MureÈan%' THEN 'Gheorghe Muresan'
            WHEN PLAYER LIKE '%Dave Cowens%' THEN 'Dave Cowens'
            WHEN PLAYER LIKE '%Hedo T%' THEN 'Hedo Turkoglu'
            ELSE PLAYER
        END AS PLAYER_FULL_NAME
        , CAST(AGE AS NUMBER) AS PLAYER_AGE
        , TEAM_ABBREVIATION
        , AWARD
    FROM source
    WHERE SEASON IS NOT NULL
)

SELECT * FROM CLS_SEASON_PLAYER_AWARDS