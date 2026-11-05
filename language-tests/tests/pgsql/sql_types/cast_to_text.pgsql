/**
[env]
namespace = true
database = true
imports = ["pgsql/sql_types/_text_columns.surql"]

[test]
reason = "a cast to text of a value evaluated per row writes PostgreSQL's text form for its type: dates, times, timestamps and intervals in the default output style, a numeric with its scale, an enum label, a range in brackets, an inet with its prefix, and a boolean as true or false"

[[test.results]]
value = "[{ d: '2026-10-27', id: 1, iv: '1 year 2 mons 3 days 04:05:06', t: '12:30:05.5', ts: '2026-10-27 12:30:00.25', tz: '2026-10-27 12:30:00+00' }, { d: NULL, id: 2, iv: '-1 days +00:00:05', t: NULL, ts: NULL, tz: NULL }]"

[[test.results]]
value = "[{ a: '192.168.1.5/32', b: 'true', big: '12345678901234567890123456789012345.12345', id: 1, m: 'ok', n: '1.50', r: '[1,6)' }, { a: '10.0.0.0/8', b: 'false', big: '0.00000', id: 2, m: 'sad', n: '-0.50', r: 'empty' }]"

[[test.results]]
value = "[{ shout: '2026-10-27!', width: 4 }]"
*/

SELECT id, d::text AS d, t::text AS t, ts::text AS ts, tz::text AS tz, iv::text AS iv FROM v ORDER BY id;

SELECT id, n::text AS n, big::text AS big, m::text AS m, r::text AS r, a::text AS a, b::text AS b FROM v ORDER BY id;

SELECT CAST(d AS varchar) || '!' AS shout, length(n::text) AS width FROM v WHERE id = 1;
