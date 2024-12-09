def update_orient_column(your_table,
                         date_miss_start, date_miss_end,
                         date_previous_start, date_previous_end,
                         column_orient):
    query = f"""WITH missing_data AS (
    SELECT
        date_origin,
        field
    FROM
        {your_table}
    WHERE
        TO_DATE(date_origin, 'YYYY-MM-DD') >= '{date_miss_start}' AND TO_DATE(date_origin, 'YYYY-MM-DD') <= '{date_miss_end}'
        AND {column_orient} = 0
),
previous_year_data AS (
    SELECT
        field,
        TO_CHAR(TO_DATE(date_origin, 'YYYY-MM-DD'), 'MM')::INTEGER AS month_origin,
        AVG({column_orient}) AS new_{column_orient}
    FROM
        {your_table}
    WHERE
        TO_DATE(date_origin, 'YYYY-MM-DD') >= '{date_previous_start}' AND TO_DATE(date_origin, 'YYYY-MM-DD') <= '{date_previous_end}'
        AND {column_orient} <> 0
    GROUP BY field, month_origin
)
UPDATE {your_table}
SET
    {column_orient} = previous_year_data.new_{column_orient}
FROM
    previous_year_data
WHERE
    TO_CHAR(TO_DATE(date_origin, 'YYYY-MM-DD'), 'MM')::INTEGER = previous_year_data.month_origin
    AND {your_table}.field = previous_year_data.field;
"""

    return query
