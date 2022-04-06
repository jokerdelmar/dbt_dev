{{
    config(tags=["daily"])
}}


/* CTE to capture user touchpoints*/
select
du.discord_user_id,
count(dm.content) as count_messages,
count(case when dm.pinned = true then dm.content end) as count_pinned_messages,
max(COALESCE(dm.edited_timestamp ,dm.timestamp)) as latest_message_ts
from {{ source('bankless','discord_user')}} du
join {{ source('bankless','discord_messages')}} dm on du.discord_user_id  = dm.author_user_id
group by 1