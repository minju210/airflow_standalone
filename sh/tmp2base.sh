#!/bin/bash

DT=$1

user="root"

MYSQL_PWD='qwer123' mysql --local-infile=1  -u"$user" <<EOF
DELETE FROM history_db.tmp_cmd_usage WHERE dt='${DT}';
INSERT INTO history_db.cmd_usage
SELECT 
	CASE WHEN dt LIKE '%-%-%'
		THEN STR_TO_DATE(dt, '%Y-%m-%d')
		ELSE STR_TO_DATE('1970-01-01', '%Y-%m-%d')
	END AS dt,
	command,
	CASE WHEN cnt REGEXP '[0-9]+$' 
		THEN CAST(cnt AS UNSIGNED)
		ELSE -1
	END AS cnt,
	'${DT}' AS tmp_dt
FROM history_db.tmp_cmd_usage
WHERE dt = '${DT}'
;
EOF

echo "DONE:)"
