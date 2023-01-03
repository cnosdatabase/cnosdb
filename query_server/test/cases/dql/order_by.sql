--#SLEEP=100
DROP TABLE IF EXISTS test;

CREATE TABLE IF NOT EXISTS test
(column1 BIGINT CODEC(DELTA),
column2 STRING CODEC(GZIP),
column3 BIGINT UNSIGNED CODEC(NULL),
column4 BOOLEAN,
column5 DOUBLE CODEC(GORILLA),
TAGS(column6, column7));

SELECT time, column1
FROM test
WHERE time < 0
ORDER BY time ASC
limit 10;
