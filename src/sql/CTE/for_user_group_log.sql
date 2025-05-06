with first_add_event as (
    select
        hk_l_user_group_activity,
        min(event_dt) as first_add_dt
    from STV2025011437__DWH.s_auth_history
    where event = 'add'
    group by hk_l_user_group_activity
),
user_group_log as (
    select
        hg.hk_group_id,
        hg.registration_dt,
        count(distinct luga.hk_user_id) as cnt_added_users
    from STV2025011437__DWH.h_groups hg
    left join STV2025011437__DWH.l_user_group_activity luga
        on luga.hk_group_id = hg.hk_group_id
    left join first_add_event fae
        on fae.hk_l_user_group_activity = luga.hk_l_user_group_activity
    group by hg.hk_group_id, hg.registration_dt
    order by hg.registration_dt
    limit 10
)
select hk_group_id, cnt_added_users
from user_group_log
order by cnt_added_users
limit 10;
