/**
[env]
imports = ["upgrade/define/3.00.00_field_temporal_retype_date_import.surql"]

[test]
upgrade = true
introduced = "3.0.0"
version = ">=3.4.0-a"
reason = """
A `datetime` field an older binary wrote can be retyped to `date` on the new
binary. Rows at exactly midnight UTC coerce on write, a row that is not at
midnight is refused with the cast to use and is left unchanged, and a backfill
with `<date>` leaves every row holding a date. The new kinds are reachable only
through the retype, so no other axis exercises how the new binary reads and
rewrites values an old writer stored."""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "['datetime', 'datetime', 'datetime', 'datetime']"

[[test.results]]
value = "[['date', 'date']]"

[[test.results]]
value = "['date']"

[[test.results]]
error = "Couldn't coerce value for field `f` of `rd:3`: Expected `date (a datetime that is not midnight UTC needs the cast `<date>`)` but found `d'2026-10-28T12:30:00Z'`"

[[test.results]]
error = "Couldn't coerce value for field `f` of `rd:4`: Expected `date (a datetime that is not midnight UTC needs the cast `<date>`)` but found `d'2026-10-29T00:00:00.500Z'`"

[[test.results]]
value = "[{ f: d'2026-10-28T12:30:00Z', id: rd:3, o: d'2026-10-29T00:00:00Z' }, { f: d'2026-10-29T00:00:00.500Z', id: rd:4 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ f: '2026-10-27', id: rd:1, o: '2026-10-28' }, { f: '2026-10-28', id: rd:2, o: 'NONE' }, { f: '2026-10-28', id: rd:3, o: '2026-10-29' }, { f: '2026-10-29', id: rd:4, o: 'NONE' }]"

[[test.results]]
value = "['date', 'date', 'date', 'date']"

[[test.results]]
value = "['date']"

[[test.results]]
error = "Couldn't coerce value for field `f` of `rd:6`: Expected `date (a datetime that is not midnight UTC needs the cast `<date>`)` but found `d'2026-11-01T01:00:00Z'`"
*/
-- Retype both fields; existing rows are not touched by the definition.
DEFINE FIELD OVERWRITE f ON rd TYPE date;
DEFINE FIELD OVERWRITE o ON rd TYPE option<date>;
-- The stored values are still datetimes and read back unchanged.
SELECT VALUE type::of(f) FROM rd ORDER BY id;
-- Midnight values coerce on write, including through the option.
UPDATE rd:1 SET f = f, o = o RETURN VALUE [type::of(f), type::of(o)];
UPDATE rd:2 SET f = f RETURN VALUE type::of(f);
-- A value that is not at midnight UTC is refused with the cast to use.
UPDATE rd:3 SET f = f;
UPDATE rd:4 SET f = f;
-- The refused rows still hold their original datetimes.
SELECT * FROM rd:3, rd:4;
-- The backfill with the cast gives every row a date, the UTC calendar day.
UPDATE rd SET f = <date> f, o = <option<date>> o RETURN NONE;
SELECT id, <string> f AS f, <string> o AS o FROM rd ORDER BY id;
SELECT VALUE type::of(f) FROM rd ORDER BY id;
-- The retyped field accepts a date and refuses a datetime off midnight.
CREATE rd:5 SET f = pd'2026-11-01' RETURN VALUE type::of(f);
CREATE rd:6 SET f = d'2026-11-01T01:00:00Z';
