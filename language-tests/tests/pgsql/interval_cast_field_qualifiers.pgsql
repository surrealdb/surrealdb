/**
[env]
namespace = true
database = true

[test]
reason = "A cast to an interval with a field qualifier or a precision truncates toward zero below the finest field and rounds the seconds half away from zero, as PostgreSQL's AdjustIntervalForTypmod does; NULL stays NULL and the column reads as interval"

[[test.results]]
value = "[{ d: ivl'P1Y2M3D', ds: ivl'P1DT2H3M4.56S', h: ivl'P1DT2H', hm: ivl'P1DT2H3M', m: ivl'P1Y2M', ms1: ivl'P1DT2H3M4.6S', s0: ivl'PT7S', y: ivl'P1Y' }]"

[[test.results]]
value = "[{ h: ivl'PT-4H', mi: ivl'PT-5M', p1: ivl'PT-1.3S', s0: ivl'PT-7S', y: ivl'P-1Y' }]"

[[test.results]]
value = "[{ d: ivl'P3D', h: ivl'PT2H', n: NULL }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: 1, iv: ivl'P1DT2H3M4.567S' }, { id: 2, iv: ivl'PT-2H-59M' }]"

[[test.results]]
value = "[{ h: ivl'P1DT2H', id: 1, p: ivl'P1DT2H3M4.6S' }, { h: ivl'PT-2H', id: 2, p: ivl'PT-2H-59M' }]"
*/

SELECT CAST('1 year 2 months 3 days 04:05:06.789' AS INTERVAL YEAR) AS y, CAST('1 year 2 months 3 days 04:05:06.789' AS INTERVAL MONTH) AS m, CAST('1 year 2 months 3 days 04:05:06.789' AS INTERVAL DAY) AS d, CAST('1 day 02:03:04' AS INTERVAL HOUR) AS h, CAST('1 day 02:03:04' AS INTERVAL HOUR TO MINUTE) AS hm, CAST('1 day 02:03:04.56' AS INTERVAL DAY TO SECOND) AS ds, CAST('1 day 02:03:04.56' AS INTERVAL MINUTE TO SECOND(1)) AS ms1, CAST('00:00:06.5' AS INTERVAL SECOND(0)) AS s0;

SELECT CAST('-1 year -11 months' AS INTERVAL YEAR) AS y, CAST('-04:59:59' AS INTERVAL HOUR) AS h, CAST('-00:05:59.9' AS INTERVAL MINUTE) AS mi, CAST('-00:00:06.5' AS INTERVAL SECOND(0)) AS s0, CAST('-00:00:01.25' AS interval(1)) AS p1;

SELECT CAST(NULL AS INTERVAL HOUR) AS n, '3 days 04:05:06'::interval day AS d, CAST(CAST('2 hours 30 minutes' AS interval) AS INTERVAL HOUR) AS h;

CREATE TABLE ivc (id INT PRIMARY KEY, iv INTERVAL);

INSERT INTO ivc (id, iv) VALUES (1, CAST('1 day 02:03:04.567' AS interval)), (2, CAST('-02:59:00' AS interval));

SELECT id, CAST(iv AS INTERVAL HOUR) AS h, CAST(iv AS interval(1)) AS p FROM ivc ORDER BY id;
