{{
    config(tags=["daily"])
}}


/*CTE to capture Tipping information*/
select
t.timestamp,
t.from_user_id,
t.to_user_id,
t.amount
from
{{ source('bankless', 'tipping')}} t