{{
  config(
    materialized = 'table',
    tags = ['PLAYER', 'CLEANSE', 'NBA_PROJECT', 'PLAYER_ORIENTED']
  )
}}

WITH FANTASY_TABLE AS (
    
SELECT
      TRIM(player)                                  AS player
    , UPPER(TRIM(team))                             AS team
    , CAST(age AS INTEGER)                          AS age
    , CAST(games_played AS INTEGER)                 AS games_played
    , CAST(minutes AS INTEGER)                      AS minutes
    , CAST(double_double AS INTEGER)                AS double_double
    , CAST(triple_double AS INTEGER)                AS triple_double

    -- sums
    , CAST("9CAT_SUM" AS NUMBER(10,2))              AS cat_9_sum
    , CAST("11CAT_SUM" AS NUMBER(10,2))             AS cat_11_sum

    -- grades
    , TRIM(points_grade)                            AS points_grade
    , TRIM(fg_grade)                                AS fg_grade
    , TRIM("3PTM_GRADE")                            AS threeptm_grade
    , TRIM(ft_grade)                                AS ft_grade
    , TRIM(oreb_grade)                              AS oreb_grade
    , TRIM(dreb_grade)                              AS dreb_grade
    , TRIM(treb_grade)                              AS treb_grade
    , TRIM(ast_grade)                               AS ast_grade
    , TRIM(stl_grade)                               AS stl_grade
    , TRIM(blk_grade)                               AS blk_grade
    , TRIM(to_grade)                                AS to_grade
    , TRIM(dd_grade)                                AS dd_grade

    -- stats
    , CAST(points AS NUMBER(10,2))                  AS points
    , CAST(fg_made AS NUMBER(10,2))                 AS fg_made
    , CAST(fg_attempt AS NUMBER(10,2))              AS fg_attempt
    , CAST("FG%" AS NUMBER(10,4))                   AS fg_pct
    , CAST("3PTM" AS NUMBER(10,2))                  AS threeptm
    , CAST(ft_made AS NUMBER(10,2))                 AS ft_made
    , CAST(ft_attempt AS NUMBER(10,2))              AS ft_attempt
    , CAST("FT%" AS NUMBER(10,4))                   AS ft_pct
    , CAST(oreb AS NUMBER(10,2))                    AS oreb
    , CAST(dreb AS NUMBER(10,2))                    AS dreb
    , CAST(treb AS NUMBER(10,2))                    AS treb
    , CAST(ast AS NUMBER(10,2))                     AS ast
    , CAST(stl AS NUMBER(10,2))                     AS stl
    , CAST(blk AS NUMBER(10,2))                     AS blk
    , CAST("TO" AS NUMBER(10,2))                    AS turnovers

FROM {{ source('STAGING_NBA_DATA', 'STG_SEASON_PLAYER_FANTASY_2425') }}

)

SELECT * FROM FANTASY_TABLE
