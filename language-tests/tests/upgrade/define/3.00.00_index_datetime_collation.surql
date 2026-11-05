/**
[env]
imports = ["upgrade/define/3.00.00_index_datetime_collation_import.surql"]

[test]
upgrade = true
introduced = "3.0.0"
# The state under test is an index stamped below format version 4, which every
# released binary writes, so the source is bounded to those; this also keeps the
# ordinary runner away, since it matches the bound against its own version. The
# reads pin what this checkout answers from such an index, so the target is
# bounded to it.
importing-version = "<3.4.0-0"
version = ">=3.4.0-a"
reason = """
An index a released binary wrote is stamped below format version 4, and holds
each Datetime as RFC 3339 text, whose byte order is not chronological. This
checkout keeps that spelling for every key it builds for such an index, so an
equality seek finds its row, and a range bounded above reads the entries the
text order puts inside it — which for a bound on a whole second leaves out the
fractions within it (surrealdb#7493). Such an index recorded nothing about the
value shapes it stores, so a range open at its upper end is read from the table
instead and returns every row it should. Nothing has to be run for the index to
keep working after the upgrade.

The _current sibling asserts the plan these reads use and that writes after the
hop keep the index consistent."""

[[test.results]]
value = "[{ id: ts_test:0, ts: d'2026-08-28T09:00:00Z' }, { id: ts_test:1, ts: d'2026-08-28T09:00:00.152Z' }, { id: ts_test:2, ts: d'2026-08-28T09:00:00.611Z' }, { id: ts_test:3, ts: d'2026-08-28T09:00:00.917Z' }, { id: ts_test:4, ts: d'2026-08-28T09:00:01.070Z' }]"

[[test.results]]
value = "[d'2026-08-28T09:00:00Z', d'2026-08-28T09:00:00.152Z', d'2026-08-28T09:00:00.611Z', d'2026-08-28T09:00:00.917Z', d'2026-08-28T09:00:01.070Z']"

[[test.results]]
value = "[{ id: ts_test:0 }]"

[[test.results]]
value = "[{ id: ts_test:1 }]"

[[test.results]]
value = "[d'2026-08-28T09:00:00Z', d'2026-08-28T09:00:00.152Z', d'2026-08-28T09:00:00.611Z', d'2026-08-28T09:00:00.917Z', d'2026-08-28T09:00:01.070Z']"

[[test.results]]
value = "[d'2026-08-28T09:00:00.152Z', d'2026-08-28T09:00:00.611Z']"

*/
-- Table scan reads all records directly: stored records survive the hop intact.
SELECT * FROM ts_test ORDER BY id;

-- Oracle table scan orders records correctly by value.
SELECT VALUE ts FROM ts_test WITH NOINDEX ORDER BY ts ASC;

-- An exact equality seek on a whole-second timestamp finds its row.
SELECT id FROM ts_test WITH INDEX idx_ts WHERE ts = d'2026-08-28T09:00:00Z';

-- An exact equality seek on a fractional timestamp finds its row.
SELECT id FROM ts_test WITH INDEX idx_ts WHERE ts = d'2026-08-28T09:00:00.152Z';

-- A range from a whole second reads the entries whose text sorts after it:
-- `...:00Z` sorts after every `...:00.fffZ`, so the fractions are left out.
SELECT VALUE ts FROM ts_test WITH INDEX idx_ts WHERE ts >= d'2026-08-28T09:00:00Z' ORDER BY ts ASC;

-- A range up to a fraction reads the entries whose text sorts before it, which
-- leaves out the whole second.
SELECT VALUE ts FROM ts_test WITH INDEX idx_ts WHERE ts <= d'2026-08-28T09:00:00.611Z' ORDER BY ts ASC;
