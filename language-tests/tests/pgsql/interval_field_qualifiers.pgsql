/**
[env]
namespace = true
database = true
imports = ["pgsql/_interval_qualifiers.surql"]

[test]
reason = "An interval column declared with a field qualifier keeps a written interval truncated toward zero below its finest field, rounds the seconds of a SECOND(p) qualifier to p digits, and reports the qualifier in its typmod and format_type, as PostgreSQL does"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ d: ivl'P1Y2M3D', dh: ivl'P1Y2M3DT4H', ds: ivl'P1Y2M3DT4H5M6.789S', ds3: ivl'P1Y2M3DT4H5M6.79S', full_iv: ivl'P1Y2M3DT4H5M6.789S', h: ivl'P1Y2M3DT4H', hm: ivl'P1Y2M3DT4H5M', id: 1, m: ivl'P1Y2M', mi: ivl'P1Y2M3DT4H5M', ms: ivl'P1Y2M3DT4H5M7S', s: ivl'P1Y2M3DT4H5M6.8S', y: ivl'P1Y', ym: ivl'P1Y2M' }]"

[[test.results]]
value = "[{ d: NULL, dh: NULL, ds: NULL, ds3: NULL, full_iv: NULL, h: ivl'PT-4H', hm: NULL, id: 2, m: NULL, mi: ivl'PT-5M', ms: ivl'PT-7S', s: NULL, y: ivl'P-1Y', ym: NULL }]"

[[test.results]]
value = "[{ d: ivl'P1Y2M3D', dh: ivl'P1Y2M3DT4H', ds: ivl'P1Y2M3DT4H5M6.789S', ds3: ivl'P1Y2M3DT4H5M6.79S', full_iv: ivl'P1Y2M3DT4H5M6.789S', h: ivl'P1Y2M3DT4H', hm: ivl'P1Y2M3DT4H5M', id: 1, m: ivl'P1Y2M', mi: ivl'P1Y2M3DT4H5M', ms: ivl'P1Y2M3DT4H5M7S', s: ivl'P1Y2M3DT4H5M6.8S', y: ivl'P1Y', ym: ivl'P1Y2M' }, { d: NULL, dh: NULL, ds: NULL, ds3: NULL, full_iv: NULL, h: ivl'PT-4H', hm: NULL, id: 2, m: NULL, mi: ivl'PT-5M', ms: ivl'PT-7S', s: NULL, y: ivl'P-1Y', ym: NULL }]"

[[test.results]]
value = "[{ attname: 'ds', atttypid: 1186, atttypmod: 470286339, formatted: 'interval day to second(3)' }, { attname: 'f', atttypid: 1186, atttypmod: -1, formatted: 'interval' }, { attname: 'h', atttypid: 1186, atttypmod: 67174399, formatted: 'interval hour' }, { attname: 'p', atttypid: 1186, atttypmod: 2147418114, formatted: 'interval(2)' }, { attname: 'ym', atttypid: 1186, atttypmod: 458751, formatted: 'interval year to month' }]"

[[test.results]]
value = "[{ column_name: 'ds', data_type: 'interval', datetime_precision: 3, udt_name: 'interval' }, { column_name: 'f', data_type: 'interval', datetime_precision: 6, udt_name: 'interval' }, { column_name: 'h', data_type: 'interval', datetime_precision: 6, udt_name: 'interval' }, { column_name: 'p', data_type: 'interval', datetime_precision: 2, udt_name: 'interval' }, { column_name: 'ym', data_type: 'interval', datetime_precision: 6, udt_name: 'interval' }]"

[[test.results]]
value = "[{ ds: 'interval day to second(3)', p: 'interval(2)', ym: 'interval year to month' }]"
*/

CREATE TABLE ivq (id INT PRIMARY KEY, full_iv INTERVAL, y INTERVAL YEAR, m INTERVAL MONTH, ym INTERVAL YEAR TO MONTH, d INTERVAL DAY, h INTERVAL HOUR, dh INTERVAL DAY TO HOUR, mi INTERVAL MINUTE, hm INTERVAL HOUR TO MINUTE, s INTERVAL SECOND(1), ds INTERVAL DAY TO SECOND, ds3 INTERVAL DAY TO SECOND(2), ms INTERVAL MINUTE TO SECOND(0));

INSERT INTO ivq (id, full_iv, y, m, ym, d, h, dh, mi, hm, s, ds, ds3, ms) VALUES (1, CAST('1 year 2 months 3 days 04:05:06.789' AS interval), CAST('1 year 2 months 3 days 04:05:06.789' AS interval), CAST('1 year 2 months 3 days 04:05:06.789' AS interval), CAST('1 year 2 months 3 days 04:05:06.789' AS interval), CAST('1 year 2 months 3 days 04:05:06.789' AS interval), CAST('1 year 2 months 3 days 04:05:06.789' AS interval), CAST('1 year 2 months 3 days 04:05:06.789' AS interval), CAST('1 year 2 months 3 days 04:05:06.789' AS interval), CAST('1 year 2 months 3 days 04:05:06.789' AS interval), CAST('1 year 2 months 3 days 04:05:06.789' AS interval), CAST('1 year 2 months 3 days 04:05:06.789' AS interval), CAST('1 year 2 months 3 days 04:05:06.789' AS interval), CAST('1 year 2 months 3 days 04:05:06.789' AS interval));

INSERT INTO ivq (id, y, h, mi, ms) VALUES (2, CAST('-1 year -11 months -3 days' AS interval), CAST('-04:59:59' AS interval), CAST('-00:05:59.9' AS interval), CAST('-00:00:06.5' AS interval));

SELECT id, full_iv, y, m, ym, d, h, dh, mi, hm, s, ds, ds3, ms FROM ivq ORDER BY id;

SELECT a.attname, a.atttypid, a.atttypmod, format_type(a.atttypid, a.atttypmod) AS formatted FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid WHERE c.relname = 'ivq_catalog' AND a.attnum > 0 AND a.attname <> 'id' ORDER BY a.attname;

SELECT column_name, data_type, udt_name, datetime_precision FROM information_schema.columns WHERE table_name = 'ivq_catalog' AND column_name <> 'id' ORDER BY column_name;

SELECT format_type(1186, 470286339) AS ds, format_type(1186, 458751) AS ym, format_type(1186, 2147418114) AS p;
