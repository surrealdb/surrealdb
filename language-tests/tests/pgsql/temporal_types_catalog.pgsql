/**
[env]
namespace = true
database = true
imports = ["pgsql/_temporal_columns.surql"]

[test]
reason = "Columns of the kinds date, time, timestamp, datetime and interval are reported by information_schema under the PostgreSQL type each maps to, a column created by CREATE TABLE with date, time, timetz, timestamp, timestamptz and interval types holds values of those kinds (timetz is a time, with its offset applied), and pg_type lists their OIDs"

[[test.results]]
value = "[{ column_name: 'id', data_type: 'text' }, { column_name: 'd', data_type: 'date' }, { column_name: 'days', data_type: 'ARRAY' }, { column_name: 'iv', data_type: 'interval' }, { column_name: 'maybe', data_type: 'date' }, { column_name: 't', data_type: 'time without time zone' }, { column_name: 'ts', data_type: 'timestamp without time zone' }, { column_name: 'tstz', data_type: 'timestamp with time zone' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ d: pd'2026-10-27', id: 1, iv: ivl'P1Y2D', t: pt'12:30:00', ts: ts'2026-10-27T12:30:00', tstz: d'2026-10-27T12:30:00Z', ttz: pt'10:30:00' }]"

[[test.results]]
value = "[{ d: 'date', iv: 'interval', t: 'time without time zone', ts: 'timestamp without time zone', tstz: 'timestamp with time zone', ttz: 'time without time zone' }]"

[[test.results]]
value = "[{ d: pd'2026-10-27', iv: ivl'P1Y2D', t: pt'12:30:00', ts: ts'2026-10-27T12:30:00', tstz: d'2026-10-27T12:30:00Z', ttz: pt'10:30:00' }]"

[[test.results]]
value = "[{ oid: 1082, typname: 'date' }, { oid: 1083, typname: 'time' }, { oid: 1114, typname: 'timestamp' }, { oid: 1184, typname: 'timestamptz' }, { oid: 1186, typname: 'interval' }]"

*/

SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'temporal_catalog' ORDER BY ordinal_position;

CREATE TABLE temporal_declared (id INT PRIMARY KEY, d DATE, t TIME, ttz TIMETZ, ts TIMESTAMP, tstz TIMESTAMPTZ, iv INTERVAL);

INSERT INTO temporal_declared (id, d, t, ttz, ts, tstz, iv) VALUES (1, CAST('2026-10-27' AS date), CAST('12:30:00' AS time), CAST('12:30:00+02' AS timetz), CAST('2026-10-27 12:30:00' AS timestamp), CAST('2026-10-27 12:30:00+00' AS timestamptz), CAST('1 year 2 days' AS interval));

SELECT pg_typeof(d) AS d, pg_typeof(t) AS t, pg_typeof(ttz) AS ttz, pg_typeof(ts) AS ts, pg_typeof(tstz) AS tstz, pg_typeof(iv) AS iv FROM temporal_declared;

SELECT d, t, ttz, ts, tstz, iv FROM temporal_declared;

SELECT typname, oid FROM pg_type WHERE typname IN ('date', 'time', 'timestamp', 'timestamptz', 'interval') ORDER BY oid;
