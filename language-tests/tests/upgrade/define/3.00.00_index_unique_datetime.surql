/**
[env]
imports = ["upgrade/define/3.00.00_index_unique_datetime_import.surql"]

[test]
upgrade = true
introduced = "3.0.0"
reason = """
A UNIQUE index over a Datetime field refuses a duplicate after the hop, and
frees a value's slot when the record holding it moves or goes. An index keeps
the spelling of a Datetime it was written with, so the slot a write checks after
the hop is the one the old binary wrote for the same instant: a duplicate
collides with it rather than landing beside it, and an update or a delete
removes it rather than leaving it to refuse the next record that takes the
value. Only equality seeks are asserted, so the reads hold whichever spelling
the index has."""

[[test.results]]
error = true

[[test.results]]
error = true

[[test.results]]
error = true

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[ts_unique:3]"

[[test.results]]
value = "[ts_unique:4]"

[[test.results]]
value = "[ts_unique:1]"

*/
-- The same instant again, as written, as a fraction, and at another offset:
-- each collides with the slot the old binary wrote.
CREATE ts_unique:3 SET at = d'2026-08-28T09:00:00Z';
CREATE ts_unique:3 SET at = d'2026-08-28T09:00:00.152Z';
CREATE ts_unique:3 SET at = d'2026-08-28T11:00:00+02:00';

-- Move one record off its value and delete the other.
UPDATE ts_unique:1 SET at = d'2026-08-28T09:00:05Z' RETURN NONE;
DELETE ts_unique:2;

-- Both freed values take a new record, which a slot left behind would refuse.
CREATE ts_unique:3 SET at = d'2026-08-28T09:00:00Z' RETURN NONE;
CREATE ts_unique:4 SET at = d'2026-08-28T09:00:00.152Z' RETURN NONE;

-- An equality seek finds each value's one record.
SELECT VALUE id FROM ts_unique WITH INDEX uniq_at WHERE at = d'2026-08-28T09:00:00Z';
SELECT VALUE id FROM ts_unique WITH INDEX uniq_at WHERE at = d'2026-08-28T09:00:00.152Z';
SELECT VALUE id FROM ts_unique WITH INDEX uniq_at WHERE at = d'2026-08-28T09:00:05Z';
