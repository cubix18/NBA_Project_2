select
    player_name,
    {{ normalize_name('player_name') }} as player_name_norm
from {{ ref('players') }}