CREATE TABLE IF NOT EXISTS dSw_by_Q_inj (
date_origin VARCHAR(7),
field INTEGER,
sum_q_inj FLOAT,
dSw FLOAT
);

SELECT create_distributed_table('dSw_by_Q_inj', 'field');