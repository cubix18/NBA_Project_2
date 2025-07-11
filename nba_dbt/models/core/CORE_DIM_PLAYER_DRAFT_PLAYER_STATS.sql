with base as (
    select * from {{ ref('CLS_DRAFT_PLAYER_STATS') }}
)

SELECT 
    PLAYER_ID
    , PLAYER_NAME
    , DRAFT_YEAR
FROM base