{{
    config(tags=["daily"])
}}


/* CTE to capture coordinape TOKE transactions*/
select du.discord_user_id,
sum(case when ce.recipient_id::int = cn.coordinape_user_id then ce.tokens  else 0 end) as total_toke_received,
sum(case when ce.sender_id::int = cn.coordinape_user_id then ce.tokens  else 0 end) as total_toke_sent
from
{{ source('bankless','discord_user')}} du
left join {{ source('bankless','coordinape_nodes')}} cn on du.discord_user_id = cn.discord_user_id
left join {{ source('bankless','coordinape_edges')}} ce on (cn.coordinape_user_id = ce.sender_id::int  or cn.coordinape_user_id = ce.recipient_id::int)
group by 1