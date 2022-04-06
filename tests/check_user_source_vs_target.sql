with source as(
    select distinct discord_user_id 
    from {{ source('bankless', 'discord_user')}}
),
target as (
    select distinct discord_user_id 
    from {{ ref('user_analytics')}}
)
select distinct source.discord_user_id
from source
where source.discord_user_id not in (select target.discord_user_id from target)