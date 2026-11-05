/**
[env]
imports = ["upgrade/define/3.00.00_index_datetime_array_import.surql"]

[test]
upgrade = true
introduced = "3.0.0"
reason = """
An index over the elements of a Datetime array (`FIELDS ts.*`) keeps working
across the hop, whichever spelling of a Datetime its keys hold. Every read here
is paired with its `WITH NOINDEX` twin and sorted, and asserts only what holds
under either spelling: a seek on an element finds each record carrying it; an
update that replaces a record's elements and a delete leave no entry behind, so
a seek on an element no record carries any more finds nothing, and a UNIQUE
index frees the slots of every element the update or delete removed; and a
UNIQUE index refuses a record carrying an element another holds, as written, at
another precision and at another offset, while accepting a new element."""

[[test.results]]
value = "[arr:1, arr:2]"

[[test.results]]
value = "[arr:1, arr:2]"

[[test.results]]
value = "[arr:1]"

[[test.results]]
value = "[arr:1]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[arr:2]"

[[test.results]]
value = "[arr:2]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[arr:1, arr:4]"

[[test.results]]
value = "[arr:1, arr:4]"

[[test.results]]
value = "[arr:4]"

[[test.results]]
value = "[arr:4]"

[[test.results]]
error = "Database index `ix` already contains d'2026-08-28T09:00:00Z', with record `arru:1`"

[[test.results]]
error = "Database index `ix` already contains d'2026-08-28T09:00:00Z', with record `arru:1`"

[[test.results]]
error = "Database index `ix` already contains d'2026-08-28T09:00:00Z', with record `arru:1`"

[[test.results]]
error = "Database index `ix` already contains d'2026-08-28T09:00:00.152Z', with record `arru:1`"

[[test.results]]
value = "[arru:9]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[arru:10]"

[[test.results]]
value = "[arru:11]"

[[test.results]]
error = "Database index `ix` already contains d'2026-08-28T09:00:05Z', with record `arru:1`"

[[test.results]]
value = "[arru:10]"

[[test.results]]
value = "[arru:10]"

[[test.results]]
value = "[arru:1]"

[[test.results]]
value = "[arru:1]"
*/
-- Seeks on elements of the non-unique index: one carried by two records, one by
-- one record, each record once.
SELECT VALUE id FROM arr WITH INDEX ix WHERE ts CONTAINS d'2026-08-28T09:00:00.152Z' ORDER BY id;
SELECT VALUE id FROM arr WITH NOINDEX WHERE ts CONTAINS d'2026-08-28T09:00:00.152Z' ORDER BY id;
SELECT VALUE id FROM arr WITH INDEX ix WHERE ts CONTAINS d'2026-08-28T09:00:00Z' ORDER BY id;
SELECT VALUE id FROM arr WITH NOINDEX WHERE ts CONTAINS d'2026-08-28T09:00:00Z' ORDER BY id;

-- Replace one pre-hop record's elements, and delete another.
UPDATE arr:1 SET ts = [d'2026-08-28T09:00:05Z'] RETURN NONE;
DELETE arr:3;

-- The elements they carried: the element arr:1 shared with arr:2 is now carried
-- by arr:2 alone, and the others by no record.
SELECT VALUE id FROM arr WITH INDEX ix WHERE ts CONTAINS d'2026-08-28T09:00:00.152Z' ORDER BY id;
SELECT VALUE id FROM arr WITH NOINDEX WHERE ts CONTAINS d'2026-08-28T09:00:00.152Z' ORDER BY id;
SELECT VALUE id FROM arr WITH INDEX ix WHERE ts CONTAINS d'2026-08-28T09:00:00Z' ORDER BY id;
SELECT VALUE id FROM arr WITH NOINDEX WHERE ts CONTAINS d'2026-08-28T09:00:00Z' ORDER BY id;
SELECT VALUE id FROM arr WITH INDEX ix WHERE ts CONTAINS d'2026-08-28T09:00:01.070Z' ORDER BY id;
SELECT VALUE id FROM arr WITH NOINDEX WHERE ts CONTAINS d'2026-08-28T09:00:01.070Z' ORDER BY id;

-- The new element is found, and a record written now is found by its elements.
CREATE arr:4 SET ts = [d'2026-08-28T09:00:00Z', d'2026-08-28T09:00:05Z'] RETURN NONE;
SELECT VALUE id FROM arr WITH INDEX ix WHERE ts CONTAINS d'2026-08-28T09:00:05Z' ORDER BY id;
SELECT VALUE id FROM arr WITH NOINDEX WHERE ts CONTAINS d'2026-08-28T09:00:05Z' ORDER BY id;
SELECT VALUE id FROM arr WITH INDEX ix WHERE ts CONTAINS d'2026-08-28T09:00:00Z' ORDER BY id;
SELECT VALUE id FROM arr WITH NOINDEX WHERE ts CONTAINS d'2026-08-28T09:00:00Z' ORDER BY id;

-- UNIQUE: an element another record holds is refused as written, at another
-- precision, at another offset, and as the second element of a new record.
CREATE arru:9 SET ts = [d'2026-08-28T09:00:00Z'];
CREATE arru:9 SET ts = [d'2026-08-28T09:00:00.000Z'];
CREATE arru:9 SET ts = [d'2026-08-28T11:00:00+02:00'];
CREATE arru:9 SET ts = [d'2026-08-28T09:00:07Z', d'2026-08-28T09:00:00.152Z'];

-- A new element is accepted, including the one the last refused write carried
-- ahead of its conflicting element: that write left no entry behind.
CREATE arru:9 SET ts = [d'2026-08-28T09:00:07Z'] RETURN VALUE id;

-- Replace one pre-hop record's elements and delete another: every element they
-- carried is free again, and the new one is held.
UPDATE arru:1 SET ts = [d'2026-08-28T09:00:05Z'] RETURN NONE;
DELETE arru:2;
CREATE arru:10 SET ts = [d'2026-08-28T09:00:00Z', d'2026-08-28T09:00:00.152Z'] RETURN VALUE id;
CREATE arru:11 SET ts = [d'2026-08-28T09:00:02Z'] RETURN VALUE id;
CREATE arru:12 SET ts = [d'2026-08-28T09:00:05Z'];

-- Seeks on the UNIQUE index.
SELECT VALUE id FROM arru WITH INDEX ix WHERE ts CONTAINS d'2026-08-28T09:00:00.152Z' ORDER BY id;
SELECT VALUE id FROM arru WITH NOINDEX WHERE ts CONTAINS d'2026-08-28T09:00:00.152Z' ORDER BY id;
SELECT VALUE id FROM arru WITH INDEX ix WHERE ts CONTAINS d'2026-08-28T09:00:05Z' ORDER BY id;
SELECT VALUE id FROM arru WITH NOINDEX WHERE ts CONTAINS d'2026-08-28T09:00:05Z' ORDER BY id;
