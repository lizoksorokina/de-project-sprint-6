drop table if exists STV2025011437__DWH.s_auth_history
CREATE TABLE STV2025011437__DWH.s_auth_history (
    hk_l_user_group_activity VARCHAR(50) PRIMARY KEY,
    user_id_from INT,
    event VARCHAR(10) NOT NULL CHECK (event IN ('create', 'add', 'leave')),
    event_dt TIMESTAMP NOT NULL,
    load_dt TIMESTAMP NOT NULL,
    load_src VARCHAR(20) NOT NULL,
    FOREIGN KEY (hk_l_user_group_activity) REFERENCES STV2025011437__DWH.l_user_group_activity(hk_l_user_group_activity)
);

