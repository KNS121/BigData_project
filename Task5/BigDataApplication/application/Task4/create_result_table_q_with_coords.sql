CREATE TABLE IF NOT EXISTS Q_with_coords (
        date_origin VARCHAR(7),
        field INTEGER,
        well TEXT,
        sum_q_prod FLOAT,
        coordx FLOAT,
        coordy FLOAT
        
    );
SELECT create_distributed_table('Q_with_coords', 'field');
