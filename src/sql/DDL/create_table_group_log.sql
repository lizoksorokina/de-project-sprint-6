drop table if exists STV2025011437__STAGING.group_log;
CREATE TABLE STV2025011437__STAGING.group_log (
    group_id  INT  NOT NULL,
    user_id  INT NOT NULL,
    user_id_from  INT,
    event         VARCHAR(10) NOT NULL CHECK (event IN ('create', 'add', 'leave')),
    datetime      TIMESTAMP NOT NULL,
    FOREIGN KEY (group_id) REFERENCES STV2025011437__STAGING.groups (id),
    FOREIGN KEY (user_id) REFERENCES STV2025011437__STAGING.users (id)
);
