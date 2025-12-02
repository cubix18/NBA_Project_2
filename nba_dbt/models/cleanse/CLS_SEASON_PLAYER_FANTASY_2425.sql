{{
  config(
    materialized = 'table',
    tags = ['PLAYER', 'CLEANSE', 'NBA_PROJECT', 'PLAYER_ORIENTED']
  )
}}

WITH FANTASY_TABLE AS (
    
SELECT
      TRIM(player)                                  AS PLAYER_FULL_NAME
    , UPPER(TRIM(team))                             AS TEAM_ABBREVIATION
    , CAST(age AS INTEGER)                          AS PLAYER_AGE
    , CAST(games_played AS INTEGER)                 AS PLAYER_GAMES_PLAYED
    , CAST(minutes AS INTEGER)                      AS PLAYER_MINUTES_PLAYED
    , CAST(double_double AS INTEGER)                AS PLAYER_DOUBLE_DOUBLE
    , CAST(triple_double AS INTEGER)                AS PLAYER_TRIPLE_DOUBLE

    -- sums
    , CAST("9CAT_SUM" AS NUMBER(10,2))              AS PLAYER_CATEGORY_9_LEAGUE
    , CAST("11CAT_SUM" AS NUMBER(10,2))             AS PLAYER_CATEGORY_11_LEAGUE

    -- grades
    , TRIM(points_grade)                            AS PLAYER_POINTS_GRADE
    , TRIM(fg_grade)                                AS PLAYER_FG_GRADE
    , TRIM("3PTM_GRADE")                            AS PLAYER_3PTM_GRADE
    , TRIM(ft_grade)                                AS PLAYER_FT_GRADE
    , TRIM(oreb_grade)                              AS PLAYER_OREB_GRADE
    , TRIM(dreb_grade)                              AS PLAYER_DREB_GRADE
    , TRIM(treb_grade)                              AS PLAYER_TREB_GRADE
    , TRIM(ast_grade)                               AS PLAYER_AST_GRADE
    , TRIM(stl_grade)                               AS PLAYER_STL_GRADE
    , TRIM(blk_grade)                               AS PLAYER_BLK_GRADE
    , TRIM(to_grade)                                AS PLAYER_TO_GRADE
    , TRIM(dd_grade)                                AS PLAYER_DD_GRADE

    -- stats
    , CAST(points AS NUMBER(10,2))                  AS PLAYER_POINTS
    , CAST(fg_made AS NUMBER(10,2))                 AS PLAYER_FG_MADE
    , CAST(fg_attempt AS NUMBER(10,2))              AS PLAYER_FG_ATTEMPTED
    , CAST("FG%" AS NUMBER(10,4))                   AS PLAYER_FG_PCT
    , CAST("3PTM" AS NUMBER(10,2))                  AS PLAYER_3PT_MADE
    , CAST(ft_made AS NUMBER(10,2))                 AS PLAYER_FT_MADE
    , CAST(ft_attempt AS NUMBER(10,2))              AS PLAYER_FT_ATTEMPTED
    , CAST("FT%" AS NUMBER(10,4))                   AS PLAYER_FT_PCT
    , CAST(oreb AS NUMBER(10,2))                    AS PLAYER_OFF_REB
    , CAST(dreb AS NUMBER(10,2))                    AS PLAYER_DEF_REB
    , CAST(treb AS NUMBER(10,2))                    AS PLAYER_TOT_REB
    , CAST(ast AS NUMBER(10,2))                     AS PLAYER_ASSISTS
    , CAST(stl AS NUMBER(10,2))                     AS PLAYER_STEALS
    , CAST(blk AS NUMBER(10,2))                     AS PLAYER_BLOCKS
    , CAST("TO" AS NUMBER(10,2))                    AS PLAYER_TOVERS
    
    , TO_TIMESTAMP(CURRENT_TIMESTAMP ) AS LOAD_DATE
    , 'STAGING_NBA_DATA.STG_SEASON_PLAYER_DATASET' AS SOURCE_TABLE

FROM {{ source('STAGING_NBA_DATA', 'STG_SEASON_PLAYER_FANTASY_2425') }}

)

SELECT * FROM FANTASY_TABLE
