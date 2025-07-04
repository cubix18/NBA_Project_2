with base as (
    select * from {{ ref('CLS_NBA_DRAFT_DATA') }}
)

SELECT 
    PLAYER_ID
    , PLAYER_NAME
    , DRAFT_YEAR
FROM base