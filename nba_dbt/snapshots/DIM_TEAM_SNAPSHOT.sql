{% snapshot DIM_TEAM_SNAPSHOT %}

{{
    config(
        target_schema='snapshots',
        unique_key='TEAM_ID',
        strategy='check',
        check_cols=['TEAM_NAME', 'TEAM_ABBREVATION', 'TEAM_CITY'],
        invalidate_hard_deletes=True
    )
}}

select distinct
    TEAM_ID,
    TEAM_NAME,
    TEAM_ABBREVATION,
    TEAM_CITY
from {{ ref('CLS_REG_SEASON_STATS_ALL') }}

{% endsnapshot %}
