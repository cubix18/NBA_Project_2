with base as (
    select * from {{ ref('CLS_DRAFT_PLAYER_STATS') }}
)

SELECT 
    DISTINCT MD5(COLLEGE_NAME) AS COLLEGE_ID
    , COLLEGE_NAME
FROM base