with source as (
    select * 
    from {{ source('STAGING_NBA_DATA', 'STG_SEASON_PLAYER_AWARDS') }}
),

CLS_SEASON_PLAYER_AWARDS as (
    SELECT 
        TRIM(SEASON) AS PLAYER_SEASON_PLAYED
        , CASE
            WHEN PLAYER_FULL_NAME LIKE '%Nikola Jok%' THEN 'Nikola Jokic'
            WHEN PLAYER_FULL_NAME LIKE '%Manu Gin%' THEN 'Manu Ginobili'
            WHEN PLAYER_FULL_NAME LIKE '%Toni Kuko%' THEN 'Toni Kukoc'
            WHEN PLAYER_FULL_NAME LIKE '%Jason Kidd (Tie)%' THEN 'Jason Kidd'
            WHEN PLAYER_FULL_NAME LIKE '%Steve Francis (Tie)%' THEN 'Steve Francis'
            WHEN PLAYER_FULL_NAME LIKE '%Goran Drag%' THEN 'Goran Dragic'
            WHEN PLAYER_FULL_NAME LIKE '%Luka Don%' THEN 'Luka Doncic'
            WHEN PLAYER_FULL_NAME LIKE '%Gheorghe MureÈan%' THEN 'Gheorghe Muresan'
            WHEN PLAYER_FULL_NAME LIKE '%Dave Cowens%' THEN 'Dave Cowens'
            WHEN PLAYER_FULL_NAME LIKE '%Hedo T%' THEN 'Hedo Turkoglu'
            ELSE PLAYER_FULL_NAME
        END AS PLAYER_FULL_NAME
        , CAST(AGE AS NUMBER) AS PLAYER_AGE
        , TEAM_ABBREVIATION
        , AWARD
    FROM source
    WHERE SEASON IS NOT NULL
)

SELECT * FROM CLS_SEASON_PLAYER_AWARDS