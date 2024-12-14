CREATE TABLE IF NOT EXISTS debit_and_p (
    field INTEGER,
    date_origin VARCHAR(7),
    sum_q_prod FLOAT,
    sum_q_inj FLOAT,
    diff_Q FLOAT,
    avg_p_plast FLOAT);
	
SELECT create_distributed_table('debit_and_p', 'field');
