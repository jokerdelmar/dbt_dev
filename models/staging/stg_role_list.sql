/* CTE to list out all user roles tagged to user*/
select
 du.discord_user_id,
 sum(case when dr.role_name ilike '%Guild%' then 1 else 0 end) as guild_membership_count,
 STRING_AGG (dr.role_name, ', ') as role_list
from {{ source('bankless', 'discord_user')}} du
 join {{ source('bankless', 'discord_user_roles')}} dur on du.discord_user_id = dur.discord_user_id
 join {{ source('bankless', 'discord_roles')}} dr on dur.discord_role_id  = dr.discord_role_id
where 
dur.active = true
group by
 1