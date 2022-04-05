with
role_list as (
    select * from {{ ref('stg_role_list')}}
),
user_messages as (
    select * from {{ ref('stg_user_messages')}}
),
user_voice_chat as (
    select * from {{ ref('stg_user_voice_chat')}}
),
tipping as (
    select * from {{ ref('stg_tipping')}}
),
user_toke as (
    select * from {{ ref('stg_user_toke')}}
),
latest_username as (
    select * from {{ ref('stg_latest_username')}}
)
/*Main Query*/
SELECT
du.discord_user_id,
lu.username,
lu.server_joined_ts,
DATE_PART('day',now() - lu.server_joined_ts) days_since_joined,
case when uvc.latest_voice_chat_ts > um.latest_message_ts then uvc.latest_voice_chat_ts
  else um.latest_message_ts end as latest_touchpoint_ts,
DATE_PART('day',now() - (case when uvc.latest_voice_chat_ts > um.latest_message_ts then uvc.latest_voice_chat_ts
  else um.latest_message_ts end)) days_since_latest_touchpoint,
/* Inactive Touchpoint Bin */
case when coalesce(DATE_PART('day',now() - (case when uvc.latest_voice_chat_ts > um.latest_message_ts then uvc.latest_voice_chat_ts
  else um.latest_message_ts end)),999) > 90 then '1. Over 90 Days'
  when coalesce(DATE_PART('day',now() - (case when uvc.latest_voice_chat_ts > um.latest_message_ts then uvc.latest_voice_chat_ts
  else um.latest_message_ts end)),999) > 60 then '2. 61 - 90 Days'
  when coalesce(DATE_PART('day',now() - (case when uvc.latest_voice_chat_ts > um.latest_message_ts then uvc.latest_voice_chat_ts
  else um.latest_message_ts end)),999) > 30 then '3. 31 - 60 Days'
  when coalesce(DATE_PART('day',now() - (case when uvc.latest_voice_chat_ts > um.latest_message_ts then uvc.latest_voice_chat_ts
  else um.latest_message_ts end)),999) > 15 then '4. 16 - 30 Days'
  when coalesce(DATE_PART('day',now() - (case when uvc.latest_voice_chat_ts > um.latest_message_ts then uvc.latest_voice_chat_ts
  else um.latest_message_ts end)),999) > 7 then '5. 8 - 15 Days'
  when coalesce(DATE_PART('day',now() - (case when uvc.latest_voice_chat_ts > um.latest_message_ts then uvc.latest_voice_chat_ts
  else um.latest_message_ts end)),999) > 3 then '6. 4 - 7 Days'
  else '7. 0 - 3 Days' end as days_since_latest_touchpoint_bin,
rl.guild_membership_count,
rl.role_list,
/* first quest block*/
case when rl.role_list ilike '%First Quests Welcome%' then true else false end as is_first_quest_welcome,
case when rl.role_list ilike '%First Quests Membership%' then true else false end as is_first_quest_membership,
case when rl.role_list ilike '%First Quests Scholar%' then true else false end as is_first_quest_scholar,
case when rl.role_list ilike '%First Quests Guest Pass%' then true else false end as is_first_quest_guest_pass,
case when rl.role_list ilike '%First Quest,%' or rl.role_list ilike '%, First Quest,%' or rl.role_list = 'First Quest' then true else false end as is_first_quest,
case when rl.role_list ilike '%First Quest Project%' then true else false end as is_first_quest_project,
case when rl.role_list ilike '%First Quest Start%' then true else false end as is_first_quest_start,
case when rl.role_list ilike '%First Quest Start%' then 'First Quest Start'
     when rl.role_list ilike '%First Quest Project%' then 'First Quest Project'
     when (rl.role_list ilike '%First Quest,%' or rl.role_list ilike '%, First Quest,%' or rl.role_list = 'First Quest') then 'First Quest'
     when rl.role_list ilike '%First Quests Guest Pass%' then 'First Quests Guest Pass'
     when rl.role_list ilike '%First Quests Scholar%' then 'First Quests Scholar'
    -- when rl.role_list ilike '%Firehose%' then 'First Quest Firehose'
     when rl.role_list ilike '%First Quests Membership%' then 'First Quests Membership'
     when rl.role_list ilike '%First Quests Welcome%' then 'First Quests Welcome'
     else 'No First Quest' end as latest_first_quest_achievement,
/* User Level block*/
case when role_list ilike '%, Guest Pass%' or role_list ilike 'Guest Pass,%' or role_list = 'Guest Pass' then true else false end as is_guest_pass_holder,
case when rl.role_list ilike '%Level 1%' then true else false end as is_level_1,
case when rl.role_list ilike '%Level 2%' then true else false end as is_level_2,
case when rl.role_list ilike '%Level 2%' then 'Level 2'
     when rl.role_list ilike '%Level 1%' then 'Level 1'
     when (role_list ilike '%, Guest Pass%' or role_list ilike 'Guest Pass,%' or role_list = 'Guest Pass') then 'Guest Pass' else 'Unknown' end as max_user_level,
case when rl.role_list ilike '%Server Booster%' then true else false end as is_server_booster,
/* Guild info block*/
case when rl.role_list ilike '%Guild%' then true else false end as is_guild_member,
case when rl.role_list ilike '%AV Guild%' then true else false end as is_av_guild_member,
case when rl.role_list ilike '%Analytics Guild%' then true else false end as is_analytics_guild_member,
case when rl.role_list ilike '%Design Guild%' then true else false end as is_design_guild_member,
case when rl.role_list ilike E'%Developer\'s Guild%' then true else false end as is_developers_guild_member,
case when rl.role_list ilike '%Education Guild%' then true else false end as is_education_guild_member,
case when rl.role_list ilike '%Legal Guild%' then true else false end as is_legal_guild_member,
case when rl.role_list ilike '%Marketing Guild%' then true else false end as is_marketing_guild_member,
case when rl.role_list ilike '%Research Guild%' then true else false end as is_research_guild_member,
case when rl.role_list ilike E'%Translator\'s Guild%' then true else false end as is_translators_guild_member,
case when rl.role_list ilike '%Treasury Guild%' then true else false end as is_treasury_guild_member,
case when rl.role_list ilike '%Treasury Guild Committee%' then true else false end as is_treasury_comittee_guild_member,
case when rl.role_list ilike '%Writers Guild%' then true else false end as is_writers_guild_member,
du.bot as is_bot,
um.count_messages,
um.count_pinned_messages,
utoke.total_toke_received,
utoke.total_toke_sent,
sum(case when t.to_user_id = du.discord_user_id then t.amount else 0 end) as total_bank_received,
sum(case when t.from_user_id = du.discord_user_id then t.amount else 0 end) as total_bank_sent,
max(case when t.to_user_id = du.discord_user_id then t.timestamp end) as latest_bank_received_ts,
max(case when t.from_user_id = du.discord_user_id then t.timestamp end) as latest_bank_sent_ts
from discord_user du
join latest_username lu on du.discord_user_id = lu.discord_user_id
join role_list rl on du.discord_user_id = rl.discord_user_id
left join tipping t on (t.to_user_id  = du.discord_user_id or t.from_user_id  = du.discord_user_id) -- tipping data
left join user_messages um on du.discord_user_id  = um.discord_user_id
left join user_voice_chat uvc on du.discord_user_id = uvc.discord_user_id
left join user_toke utoke on du.discord_user_id = utoke.discord_user_id -- coordinape data
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40