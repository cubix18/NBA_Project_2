with base as (
    select * from {{ ref('CLS_NBA_DRAFT_DATA') }}
)

SELECT 
    DISTINCT MD5(COLLEGE_NAME) AS COLLEGE_ID
    , COLLEGE_NAME
FROM base