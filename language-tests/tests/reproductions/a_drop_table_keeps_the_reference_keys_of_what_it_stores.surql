/**
[test]
reason = "A DROP table keeps reference keys for exactly the records storage holds. A create or an update on one stores nothing, so it writes no reference key for the record it discards and leaves those of a record stored before DROP as storage holds them; a delete purges the stored record whatever DROP says, so it releases that record's keys."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

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
value = "{ refs: [] }"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: ru:1, r: tgt:a }]"

[[test.results]]
value = "{ refs: [ru:1] }"

[[test.results]]
value = "{ refs: [] }"

[[test.results]]
value = "[]"

[[test.results]]
error = "Cannot delete `tgt:a` as it is referenced by `ru:1` with an ON DELETE REJECT clause"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "{ refs: [] }"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

*/

DEFINE TABLE tgt SCHEMALESS;
DEFINE TABLE rs DROP;
DEFINE FIELD r ON rs TYPE option<record<tgt>> REFERENCE ON DELETE REJECT;
CREATE tgt:1 RETURN NONE;
-- Each creating order discards the record, so none leaves a referrer behind.
CREATE rs:1 SET r = tgt:1 RETURN NONE;
UPSERT rs:2 SET r = tgt:1 RETURN NONE;
INSERT INTO rs { id: rs:3, r: tgt:1 } RETURN NONE;
SELECT * FROM rs;
tgt:1.{ refs: <~rs };
-- No stored record references tgt:1, so ON DELETE REJECT has nothing to refuse.
DELETE tgt:1 RETURN NONE;
SELECT * FROM tgt;

-- A record stored before its table took DROP.
DEFINE TABLE ru SCHEMALESS;
DEFINE FIELD r ON ru TYPE option<record<tgt>> REFERENCE ON DELETE REJECT;
CREATE tgt:a, tgt:b RETURN NONE;
CREATE ru:1 SET r = tgt:a RETURN NONE;
DEFINE TABLE OVERWRITE ru DROP;
-- The update is discarded: storage keeps the link to tgt:a, and so do the reference keys.
UPDATE ru:1 SET r = tgt:b RETURN NONE;
SELECT * FROM ru;
tgt:a.{ refs: <~ru };
tgt:b.{ refs: <~ru };
DELETE tgt:b RETURN NONE;
DELETE tgt:a RETURN NONE;
-- The delete purges the stored record, and releases its reference keys with it.
DELETE ru:1 RETURN NONE;
SELECT * FROM ru;
tgt:a.{ refs: <~ru };
DELETE tgt:a RETURN NONE;
SELECT * FROM tgt;
