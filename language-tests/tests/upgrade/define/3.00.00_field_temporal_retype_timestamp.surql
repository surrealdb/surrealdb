/**
[env]
imports = ["upgrade/define/3.00.00_field_temporal_retype_timestamp_import.surql"]

[test]
upgrade = true
introduced = "3.0.0"
version = ">=3.4.0-a"
reason = """
A `datetime` field an older binary wrote can be retyped to `timestamp` on the
new binary. A `datetime` narrows to a `timestamp` exactly, whatever its time of
day, so every row coerces on write with its wall time unchanged and nothing is
refused, and a `<timestamp>` backfill agrees with the coercion."""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "['datetime', 'datetime', 'datetime', 'datetime']"

[[test.results]]
value = "[['timestamp', 'timestamp']]"

[[test.results]]
value = "[['timestamp', 'timestamp']]"

[[test.results]]
value = "['timestamp', 'datetime', 'timestamp', 'datetime']"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ f: '2026-10-27T12:00:00', id: rt:1, o: '2026-10-28T00:00:00' }, { f: '2026-10-27T12:00:00.123456', id: rt:2, o: 'NONE' }, { f: '1969-12-31T23:59:59.500', id: rt:3, o: '1970-01-01T00:00:00' }, { f: '2026-10-27T00:00:00', id: rt:4, o: 'NONE' }]"

[[test.results]]
value = "['timestamp', 'timestamp', 'timestamp', 'timestamp']"

[[test.results]]
value = "['2026-10-27T12:00:00Z', '2026-10-27T12:00:00.123456Z', '1969-12-31T23:59:59.500Z', '2026-10-27T00:00:00Z']"
*/
-- Retype both fields.
DEFINE FIELD OVERWRITE f ON rt TYPE timestamp;
DEFINE FIELD OVERWRITE o ON rt TYPE option<timestamp>;
-- The stored values are still datetimes and read back unchanged.
SELECT VALUE type::of(f) FROM rt ORDER BY id;
-- A row coerces on write whatever its time of day, before and after the epoch.
UPDATE rt:1 SET f = f, o = o RETURN VALUE [type::of(f), type::of(o)];
UPDATE rt:3 SET f = f, o = o RETURN VALUE [type::of(f), type::of(o)];
-- The rows not written yet still hold datetimes.
SELECT VALUE type::of(f) FROM rt ORDER BY id;
-- The backfill with the cast takes the rest.
UPDATE rt SET f = <timestamp> f, o = <option<timestamp>> o RETURN NONE;
SELECT id, <string> f AS f, <string> o AS o FROM rt ORDER BY id;
SELECT VALUE type::of(f) FROM rt ORDER BY id;
-- The wall time is the UTC time, so the cast back is the original instant.
SELECT VALUE <string><datetime> f FROM rt ORDER BY id;
