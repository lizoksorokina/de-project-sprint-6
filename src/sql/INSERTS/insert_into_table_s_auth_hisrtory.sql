INSERT INTO STV2025011437__DWH.l_user_group_activity (
    hk_l_user_group_activity, hk_user_id, hk_group_id, load_dt, load_src)
SELECT DISTINCT
    HASH(hu.hk_user_id, hg.hk_group_id) AS hk_l_user_group_activity,
    hu.hk_user_id,
    hg.hk_group_id,
    CURRENT_TIMESTAMP AS load_dt,
    'group_log' AS load_src
from  STV2025011437__STAGING.group_log AS gl
left join STV2025011437__DWH.h_users AS hu ON gl.user_id = hu.user_id
left join STV2025011437__DWH.h_groups AS hg ON gl.group_id = hg.group_id;