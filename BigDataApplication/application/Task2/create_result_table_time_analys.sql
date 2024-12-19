CREATE TABLE IF NOT EXISTS avgtime (
            date_origin VARCHAR(7),
            field INTEGER,
            avg_time_prod FLOAT,
            avg_time_inj FLOAT
        );
SELECT create_distributed_table('avgtime', 'field');
