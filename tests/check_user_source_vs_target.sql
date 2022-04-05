with source as(
    select distinct discord_user_id 
    from {{ source('bankless', 'discord_user')}}
),
target as (
    select distinct discord_user_id 
    from {{ ref('user_analytics')}}
)
select count(distinct source.discord_user_id) as count_missing_user
from source
where source.discord_user_id not in (select target.discord_user_id from target)