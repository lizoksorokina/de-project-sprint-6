drop table if exists STV2025011437__DWH.l_user_group_activity;
CREATE TABLE STV2025011437__DWH.l_user_group_activity (
    hk_l_user_group_activity VARCHAR(50) PRIMARY KEY,
    hk_user_id INT NOT NULL,
    hk_group_id INT NOT NULL,
    load_dt TIMESTAMP NOT NULL,
    load_src VARCHAR(20) NOT NULL,
    FOREIGN KEY (hk_user_id) REFERENCES STV2025011437__DWH.h_users(hk_user_id),
    FOREIGN KEY (hk_group_id) REFERENCES STV2025011437__DWH.h_groups(hk_group_id)
);

