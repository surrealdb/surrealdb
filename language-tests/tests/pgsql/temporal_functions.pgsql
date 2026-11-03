/**
[env]
namespace = true
database = true

[test]
reason = "date_trunc, extract, date_part, age, make_interval, make_date, make_time, make_timestamp, to_date and to_char over date, time, timestamp and interval values"

[[test.results]]
value = "[{ d: d'2026-01-01T00:00:00Z', h: ts'2026-10-27T12:00:00', ts: ts'2026-10-01T00:00:00', tz: d'2026-10-26T00:00:00Z' }]"

[[test.results]]
value = "[{ century: 21, decade: 202, epoch: 1793104200.5dec, isoyear: 2026, julian: 2461341, millennium: 3, year: 2026 }]"

[[test.results]]
value = "[{ days: 3, hour: 12, hours: 4, minute: 30, second: 45.25dec }]"

[[test.results]]
value = "[{ a: ivl'P6Y7M27D', clock: ivl'PT12H30M', negative: ivl'P-6Y-7M-27D' }]"

[[test.results]]
value = "[{ d: pd'2026-10-27', iv: ivl'P1Y2M3DT4H5M6.5S', t: pt'12:30:45.500', ts: ts'2026-10-27T12:30:45.500', week: ivl'P7D' }]"

[[test.results]]
value = "[{ d: 'date', iv: 'interval', t: 'time without time zone', to_date: 'date', ts: 'timestamp without time zone' }]"

[[test.results]]
value = "[{ compact: pd'2024-03-15', d: pd'2024-03-15', short_year: pd'2024-03-15' }]"

[[test.results]]
value = "[{ d: '2026-10-27', t: '01:45 PM', ts: '15:04:05' }]"

[[test.results]]
error = "Incorrect arguments for function time::make_date(). 2026-13-1 is not a valid date."

*/

SELECT date_trunc('month', CAST('2026-10-27 12:30:45' AS timestamp)) AS ts, date_trunc('year', CAST('2026-10-27' AS date)) AS d, date_trunc('week', CAST('2026-10-27 12:30:45+00' AS timestamptz)) AS tz, date_trunc('hour', CAST('2026-10-27 12:30:45.5' AS timestamp)) AS h;

SELECT extract(year FROM CAST('2026-10-27' AS date)) AS year, extract(century FROM CAST('2026-10-27' AS date)) AS century, extract(decade FROM CAST('2026-10-27' AS date)) AS decade, extract(millennium FROM CAST('2026-10-27' AS date)) AS millennium, extract(isoyear FROM CAST('2027-01-01' AS date)) AS isoyear, extract(julian FROM CAST('2026-10-27' AS date)) AS julian, extract(epoch FROM CAST('2026-10-27 12:30:00.5' AS timestamp)) AS epoch;

SELECT extract(hour FROM CAST('12:30:45.25' AS time)) AS hour, date_part('second', CAST('12:30:45.25' AS time)) AS second, date_part('minute', CAST('2026-10-27 12:30:45' AS timestamp)) AS minute, extract(day FROM CAST('3 days 04:00:00' AS interval)) AS days, extract(hour FROM CAST('3 days 04:00:00' AS interval)) AS hours;

SELECT age(CAST('2026-10-27' AS timestamp), CAST('2020-02-29' AS timestamp)) AS a, age(CAST('2020-02-29' AS timestamp), CAST('2026-10-27' AS timestamp)) AS negative, age(CAST('2026-10-27 12:30:00' AS timestamp), CAST('2026-10-27' AS timestamp)) AS clock;

SELECT make_interval(1, 2, 0, 3, 4, 5, 6.5) AS iv, make_interval(0, 0, 1) AS week, make_date(2026, 10, 27) AS d, make_time(12, 30, 45.5) AS t, make_timestamp(2026, 10, 27, 12, 30, 45.5) AS ts;

SELECT pg_typeof(make_date(2026, 10, 27)) AS d, pg_typeof(make_time(12, 30, 45.5)) AS t, pg_typeof(make_timestamp(2026, 10, 27, 12, 30, 45)) AS ts, pg_typeof(make_interval(1)) AS iv, pg_typeof(to_date('15/03/2024', 'DD/MM/YYYY')) AS to_date;

SELECT to_date('15/03/2024', 'DD/MM/YYYY') AS d, to_date('20240315', 'YYYYMMDD') AS compact, to_date('15-03-24', 'DD-MM-YY') AS short_year;

SELECT to_char(CAST('2026-10-27' AS date), 'YYYY-MM-DD') AS d, to_char(CAST('2026-10-27 15:04:05' AS timestamp), 'HH24:MI:SS') AS ts, to_char(CAST('13:45:00' AS time), 'HH12:MI AM') AS t;

SELECT make_date(2026, 13, 1) AS bad;
