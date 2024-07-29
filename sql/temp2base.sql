USE history_db;

INSERT INTO cmd_usage
SELECT
    STR_TO_DATE(dt, '%Y-%m-%d') AS dt,
    command,
    cnt
FROM tmp_cmd_usage
WHERE dt = '2024-07-17';
