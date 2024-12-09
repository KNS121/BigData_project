def update_missing_data(your_table,
                        date_miss_start, date_miss_end,
                        date_previous_start, date_previous_end,
                        column_miss, column_orient):
    
    query = f"""WITH missing_data AS (
    SELECT
        date_origin,
        field,
        {column_orient},
        {column_miss}
    FROM
        {your_table}
    WHERE
        TO_DATE(date_origin, 'YYYY-MM-DD') >= '{date_miss_start}' AND TO_DATE(date_origin, 'YYYY-MM-DD') <= '{date_miss_end}'
        AND {column_miss} = 0 AND {column_orient} <> 0
),
previous_year_data AS (
    SELECT
        date_origin,
        field,
        {column_orient} AS {column_orient}_0,
        {column_miss} AS {column_miss}_0
    FROM
        {your_table}
    WHERE
        TO_DATE(date_origin, 'YYYY-MM-DD') >= '{date_previous_start}' AND TO_DATE(date_origin, 'YYYY-MM-DD') <= '{date_previous_end}'
        AND {column_miss} <> 0 AND {column_orient} <> 0
),
matched_data AS (
    SELECT
        missing_data.date_origin,
        missing_data.field,
        missing_data.{column_orient},
        previous_year_data.{column_miss}_0,
        previous_year_data.{column_orient}_0,
        ROW_NUMBER() OVER (PARTITION BY missing_data.date_origin, missing_data.field ORDER BY ABS(missing_data.{column_orient} - previous_year_data.{column_orient}_0)) AS rn
    FROM
        missing_data
    JOIN
        previous_year_data
    ON
        missing_data.field = previous_year_data.field
),
calculated_data AS (
    SELECT
        date_origin,
        field,
        (1 + ({column_orient} - {column_orient}_0) / {column_orient}) * {column_miss}_0 AS new_{column_miss}
    FROM
        matched_data
    WHERE
        rn = 1
)
UPDATE {your_table}
SET
    {column_miss} = calculated_data.new_{column_miss}
FROM
    calculated_data
WHERE
    {your_table}.date_origin = calculated_data.date_origin AND {your_table}.field = calculated_data.field;
"""
    
    return query