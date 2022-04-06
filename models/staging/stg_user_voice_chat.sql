{{
    config(tags=["daily"])
}}

/*  CTE for Discord voice channel */
select
vs.user_id as discord_user_id,
max(vs.inserted_at ) as latest_voice_chat_ts
from
{{ source('bankless','voice_states')}} vs
where
vs.channel_name <> 'gone'
group by 1