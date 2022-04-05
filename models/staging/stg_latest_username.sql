/*CTE for fetching the latest username and server joined timestamp*/
select
du.discord_user_id,
first_value(du.username) over (partition by du.discord_user_id order by inserted_at desc) as username,
first_value(du.joined_at) over (partition by du.discord_user_id order by inserted_at) as server_joined_ts
from
{{ source('bankless', 'discord_user')}} du