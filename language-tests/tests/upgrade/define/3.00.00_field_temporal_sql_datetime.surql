/**
[env]
imports = ["upgrade/define/3.00.00_field_temporal_sql_datetime_import.surql"]

[test]
upgrade = true
introduced = "3.0.0"
version = ">=3.4.0-a"
reason = """
A table a SQL client created on an older binary keeps `datetime` columns, and the
new binary writes a `date` or a `timestamp` into them by lossless coercion, as
midnight UTC and as UTC, while a string and a plain number are still refused.
The column is not retyped, so the value read back is a datetime."""

[[test.results]]
value = "[{ created: d'2026-10-28T00:00:00Z', id: sq:2 }]"

[[test.results]]
value = "[{ created: d'2026-10-29T10:20:30Z', id: sq:3, seen: d'2026-10-29T11:00:00.500Z' }]"

[[test.results]]
error = "Couldn't coerce value for field `created` of `sq:4`: Expected `datetime` but found `'2026-10-30T00:00:00Z'`"

[[test.results]]
error = "Couldn't coerce value for field `created` of `sq:5`: Expected `datetime` but found `1761782400`"

[[test.results]]
value = "[{ created: '2026-10-27T08:00:00Z', id: sq:1, seen: '2026-10-27T09:00:00Z' }, { created: '2026-10-28T00:00:00Z', id: sq:2, seen: NONE }, { created: '2026-10-29T10:20:30Z', id: sq:3, seen: '2026-10-29T11:00:00.500Z' }]"

[[test.results]]
value = "['datetime', 'datetime', 'datetime']"

[[test.results]]
value = "[sq:2, sq:3]"

[[test.results]]
value = "[sq:3]"
*/
-- A date widens to midnight UTC.
CREATE sq:2 SET created = pd'2026-10-28';
-- A timestamp widens to a datetime read as UTC, through the option too.
CREATE sq:3 SET created = ts'2026-10-29T10:20:30', seen = ts'2026-10-29T11:00:00.5';
-- Strings and numbers are not coerced.
CREATE sq:4 SET created = '2026-10-30T00:00:00Z';
CREATE sq:5 SET created = 1761782400;
-- The rows read back as datetimes, the old row included.
SELECT id, <string> created AS created, <option<string>> seen AS seen FROM sq ORDER BY id;
SELECT VALUE type::of(created) FROM sq ORDER BY id;
-- The existing index serves the new entries.
SELECT VALUE id FROM sq WHERE created >= d'2026-10-28T00:00:00Z' ORDER BY id;
SELECT VALUE id FROM sq WHERE created = d'2026-10-29T10:20:30Z';
