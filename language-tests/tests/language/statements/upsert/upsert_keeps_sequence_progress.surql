/**
[env]
namespace = true
database = true

[test]
reason = """
An UPSERT on a table that derives the record id from a sequence keeps that
sequence's progress when the drawn id names an existing record and the UPSERT
falls back to updating it. The id is drawn by the `id` field's DEFAULT, or by a
data clause evaluated before the record has an id (`SET id = id ?? ...`). A
sequence defined in the same transaction is restored by the rollback of the
create attempt, so the attempt's draws are replayed after it: the next UPSERT
draws the next value and creates a new record, rather than drawing the same
value and updating the same record again — the same as INSERT ... ON DUPLICATE
KEY UPDATE and as a sequence committed earlier.

Only the sequence is moved on; nothing that derived the id is evaluated again.
Whatever else a data clause did while deriving it belongs to the create attempt
and is rolled back with it, whether the fallback update then succeeds or fails,
and a DEFAULT that inspects the document or draws from a committed sequence as
well behaves as it did the first time. A data clause that draws the id even
when the record has one draws again on the update, as it would from a committed
sequence. The replay costs one draw however far the value is from where the
sequence was restored to, and skips a sequence the rollback left undefined.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[t:0]"

[[test.results]]
value = "[t:1]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: t:0, v: 1 }, { id: t:1, v: 2 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[t2:0]"

[[test.results]]
value = "[t2:1]"

[[test.results]]
value = "[{ id: t2:0, v: 1 }, { id: t2:1, v: 2 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[t3:0]"

[[test.results]]
value = "[t3:1]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: t3:0, v: 1 }, { id: t3:1, v: 2 }]"

[[test.results]]
value = "1"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "500"

[[test.results]]
value = "0"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: t4:0, v: 0 }]"

[[test.results]]
value = "0"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[t5:0]"

[[test.results]]
value = "[t5:1]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: t5:0, v: 1 }, { id: t5:1, v: 2 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[t6:[0, 0]]"

[[test.results]]
value = "[t6:[1, 1]]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: t6:[0, 0], v: 1 }, { id: t6:[1, 1], v: 2 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[t8:1000000000000]"

[[test.results]]
value = "1000000000001"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: t8:1000000000000, v: 1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[t9:0]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: t9:0, v: 1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[t10:9223372036854775757]"

[[test.results]]
value = "9223372036854775758"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: t10:9223372036854775757, v: 1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[t11:200]"

[[test.results]]
value = "201"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: t11:200, v: 1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "500"

[[test.results]]
value = "[t12:0]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: t12:0, v: 1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
error = "Found 1 for the `id` field, but a specific record has been specified"

[[test.results]]
error = "Found 1 for the `id` field, but a specific record has been specified"

[[test.results]]
value = "[{ id: t13:0, v: 0 }]"

*/

-- A sequence defined in the same transaction
BEGIN;
DEFINE SEQUENCE seq;
DEFINE TABLE t SCHEMALESS;
DEFINE FIELD id ON t DEFAULT sequence::nextval('seq');
CREATE t:0 SET v = 0 RETURN NONE;
UPSERT t SET v = 1 RETURN VALUE id;
UPSERT t SET v = 2 RETURN VALUE id;
COMMIT;
SELECT id, v FROM t ORDER BY id;

-- A sequence committed earlier
DEFINE SEQUENCE seq2;
DEFINE TABLE t2 SCHEMALESS;
DEFINE FIELD id ON t2 DEFAULT sequence::nextval('seq2');
CREATE t2:0 SET v = 0 RETURN NONE;
UPSERT t2 SET v = 1 RETURN VALUE id;
UPSERT t2 SET v = 2 RETURN VALUE id;
SELECT id, v FROM t2 ORDER BY id;

-- An id the data clause draws, in a block that writes as well: the create
-- attempt's write is rolled back with it, so only the UPSERT that creates
-- keeps one
BEGIN;
DEFINE SEQUENCE seq3;
DEFINE TABLE audit SCHEMALESS;
DEFINE TABLE t3 SCHEMALESS;
CREATE t3:0 SET v = 0 RETURN NONE;
UPSERT t3 SET id = id ?? { CREATE audit RETURN NONE; RETURN sequence::nextval('seq3'); }, v = 1 RETURN VALUE id;
UPSERT t3 SET id = id ?? { CREATE audit RETURN NONE; RETURN sequence::nextval('seq3'); }, v = 2 RETURN VALUE id;
COMMIT;
SELECT id, v FROM t3 ORDER BY id;
RETURN count(SELECT * FROM audit);

-- The same data clause, where the fallback update fails and the API handler
-- running it answers with the error: nothing the failed handler did survives,
-- its draw from the sequence the transaction defined included, so the value it
-- drew is free to be drawn again
DEFINE TABLE audit4 SCHEMALESS;
DEFINE TABLE t4 SCHEMALESS;
DEFINE EVENT refuse ON t4 WHEN $event = 'UPDATE' THEN { THROW 'refused' };
DEFINE API '/t4' FOR post THEN (UPSERT t4 SET id = id ?? { CREATE audit4 RETURN NONE; RETURN sequence::nextval('seq4'); }, v = 1);
BEGIN;
DEFINE SEQUENCE seq4;
CREATE t4:0 SET v = 0 RETURN NONE;
api::invoke('/t4', { method: 'post' }).status;
RETURN sequence::nextval('seq4');
COMMIT;
SELECT id, v FROM t4 ORDER BY id;
RETURN count(SELECT * FROM audit4);

-- A DEFAULT that inspects the document is not evaluated against the one the
-- create attempt built
BEGIN;
DEFINE SEQUENCE seq5;
DEFINE TABLE t5 SCHEMALESS;
DEFINE FIELD id ON t5 DEFAULT { IF $this.id != NONE { THROW 'default sees id' }; RETURN sequence::nextval('seq5'); };
CREATE t5:0 SET v = 0 RETURN NONE;
UPSERT t5 SET v = 1 RETURN VALUE id;
UPSERT t5 SET v = 2 RETURN VALUE id;
COMMIT;
SELECT id, v FROM t5 ORDER BY id;

-- A DEFAULT drawing from a committed sequence as well draws from it once
DEFINE SEQUENCE c6;
DEFINE TABLE t6 SCHEMALESS;
CREATE t6:[0, 0] SET v = 0 RETURN NONE;
BEGIN;
DEFINE SEQUENCE p6;
DEFINE FIELD id ON t6 DEFAULT [sequence::nextval('c6'), sequence::nextval('p6')];
UPSERT t6 SET v = 1 RETURN VALUE id;
UPSERT t6 SET v = 2 RETURN VALUE id;
COMMIT;
SELECT id, v FROM t6 ORDER BY id;

-- A data clause that redefines its sequence far ahead before drawing: the
-- replay moves the restored sequence there in one step
BEGIN;
DEFINE SEQUENCE seq8;
DEFINE TABLE t8 SCHEMALESS;
CREATE t8:1000000000000 SET v = 0 RETURN NONE;
UPSERT t8 SET id = id ?? { DEFINE SEQUENCE OVERWRITE seq8 BATCH 100 START 1000000000000; RETURN sequence::nextval('seq8'); }, v = 1 RETURN VALUE id TIMEOUT 5s;
sequence::nextval('seq8');
COMMIT;
SELECT id, v FROM t8 ORDER BY id;

-- A data clause that defines its sequence, where an earlier statement in the
-- transaction defined and removed it: the rollback leaves no definition, and
-- the replay skips it
BEGIN;
DEFINE SEQUENCE seq9;
REMOVE SEQUENCE seq9;
DEFINE TABLE t9 SCHEMALESS;
CREATE t9:0 SET v = 0 RETURN NONE;
UPSERT t9 SET id = id ?? { DEFINE SEQUENCE seq9; RETURN sequence::nextval('seq9'); }, v = 1 RETURN VALUE id;
COMMIT;
SELECT id, v FROM t9 ORDER BY id;

-- Near the top of the range, the replay claims the window that drawing one
-- value at a time would have reached, and that window stays in range
BEGIN;
DEFINE SEQUENCE seq10 BATCH 100 START 9223372036854775600;
DEFINE TABLE t10 SCHEMALESS;
CREATE t10:9223372036854775757 SET v = 0 RETURN NONE;
UPSERT t10 SET id = id ?? { DEFINE SEQUENCE OVERWRITE seq10 BATCH 1 START 9223372036854775757; RETURN sequence::nextval('seq10'); }, v = 1 RETURN VALUE id;
sequence::nextval('seq10');
COMMIT;
SELECT id, v FROM t10 ORDER BY id;

-- From the bottom of the range to above zero: the distance does not fit in a
-- signed 64-bit value, and the replay still lands on the window that holds the
-- drawn value
BEGIN;
DEFINE SEQUENCE seq11 BATCH 100 START -9223372036854775807;
DEFINE TABLE t11 SCHEMALESS;
CREATE t11:200 SET v = 0 RETURN NONE;
UPSERT t11 SET id = id ?? { DEFINE SEQUENCE OVERWRITE seq11 BATCH 1 START 200; RETURN sequence::nextval('seq11'); }, v = 1 RETURN VALUE id;
sequence::nextval('seq11');
COMMIT;
SELECT id, v FROM t11 ORDER BY id;

-- A create attempt whose schema error leaves no record to update keeps none of
-- its draw: the next UPSERT draws the same value
DEFINE TABLE t12 SCHEMAFULL;
DEFINE FIELD v ON t12 TYPE int;
DEFINE API '/t12' FOR post THEN (UPSERT t12 SET v = 'x');
BEGIN;
DEFINE SEQUENCE seq12;
DEFINE FIELD id ON t12 DEFAULT sequence::nextval('seq12');
api::invoke('/t12', { method: 'post' }).status;
UPSERT t12 SET v = 1 RETURN VALUE id;
COMMIT;
SELECT id, v FROM t12 ORDER BY id;

-- A data clause that draws the id whether or not the record has one draws
-- again on the fallback update, which then names another record: the same
-- error as with a sequence committed earlier, rather than an update of the
-- record the create attempt's draw named
DEFINE TABLE t13 SCHEMALESS;
DEFINE SEQUENCE c13;
CREATE t13:0 SET v = 0 RETURN NONE;
{ DEFINE SEQUENCE p13; UPSERT t13 SET id = sequence::nextval('p13'), v = 1 RETURN VALUE id };
UPSERT t13 SET id = sequence::nextval('c13'), v = 1 RETURN VALUE id;
SELECT id, v FROM t13 ORDER BY id;
