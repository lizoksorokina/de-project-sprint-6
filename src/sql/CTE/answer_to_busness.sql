with first_add_event as (
    select
        hk_l_user_group_activity
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
),
user_group_messages as (
    select
        lgd.hk_group_id,
        count(distinct lum.hk_user_id) as cnt_users_in_group_with_messages
    from STV2025011437__DWH.l_groups_dialogs lgd
    left join STV2025011437__DWH.l_user_message lum
        on lum.hk_message_id = lgd.hk_message_id
    group by lgd.hk_group_id
)
select
    ugl.hk_group_id,
    ugl.cnt_added_users,
    ugm.cnt_users_in_group_with_messages,
    ugm.cnt_users_in_group_with_messages / nullif(ugl.cnt_added_users, 0) as group_conversion
from user_group_log as ugl
left join user_group_messages as ugm
    on ugl.hk_group_id = ugm.hk_group_id
order by group_conversion desc;