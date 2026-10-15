/**
[test]
reason = "Change 23: REFERENCE added to, or dropped from, a field of a table that has rows is accepted by every table and mints nothing. Adding it backfills no backlink: rows stored before it have none until their value changes, and an ON DELETE action does not reach them. Dropping it purges every backlink of the field and leaves the stored values alone. The design rules do not describe this."

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
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "6"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "'DEFINE FIELD `by` ON book TYPE none | record<author> REFERENCE ON DELETE REJECT PERMISSIONS FULL'"

[[test.results]]
value = "6"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ by: author:1, id: book:1, n: 1, ni: true }, { by: author:2, id: book:2, n: 2, ni: true }, { by: author:3, id: book:3, n: 3, ni: true }]"

[[test.results]]
value = "[{ b: [], id: author:1 }, { b: [], id: author:2 }, { b: [], id: author:3 }, { b: [], id: author:4 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ b: [], id: author:1 }, { b: [], id: author:2 }, { b: [], id: author:3 }, { b: [], id: author:4 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ by: author:1, id: book:1 }, { by: author:2, id: book:2 }, { by: author:3, id: book:3 }]"

[[test.results]]
value = "[]"

[[test.results]]
error = "Cannot delete `author:2` as it is referenced by `book:4` with an ON DELETE REJECT clause"

[[test.results]]
value = "[{ b: [book:4], id: author:2 }, { b: [], id: author:3 }, { b: [], id: author:4 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ b: [book:4], id: author:2 }, { b: [book:3], id: author:3 }, { b: [], id: author:4 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ b: [], id: author:2 }, { b: [], id: author:3 }, { b: [], id: author:4 }]"

[[test.results]]
value = "[{ by: author:1, id: book:1 }, { by: author:2, id: book:2 }, { by: author:3, id: book:3 }, { by: author:2, id: book:4 }]"

[[test.results]]
value = "'DEFINE FIELD `by` ON book TYPE none | record<author> PERMISSIONS FULL'"

[[test.results]]
value = "6"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ b: [], id: author:2 }, { b: [], id: author:3 }, { b: [], id: author:4 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ b: [], id: author:2 }, { b: [book:5], id: author:3 }, { b: [], id: author:4 }]"

[[test.results]]
value = "6"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ b: [page:1], id: author:3 }]"

[[test.results]]
value = "2"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ b: [], id: author:3 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ b: [], id: author:3 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ database: 'd3', namespace: 'test' }"

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
value = "NONE"

[[test.results]]
value = "[{ b: [], id: author:1 }, { b: [], id: author:2 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ b: [], id: author:1 }, { b: [book:1, book:2], id: author:2 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ b: [], id: author:1 }, { b: [], id: author:2 }]"

[[test.results]]
value = "[{ by: author:2, id: book:1 }, { by: author:2, id: book:2 }]"
*/
-- Enforced table with rows. `n` is Guaranteed by a migration so the state
-- the change must keep is visible.
DEFINE TABLE author SCHEMAFULL;
DEFINE TABLE book SCHEMAFULL;
DEFINE FIELD by ON book TYPE option<record<author>>;
DEFINE FIELD title ON book TYPE option<string>;
DEFINE FIELD n ON book TYPE string;
CREATE author:1, author:2, author:3, author:4 RETURN NONE;
CREATE book:1 SET by = author:1, n = '1' RETURN NONE;
CREATE book:2 SET by = author:2, n = '2' RETURN NONE;
CREATE book:3 SET by = author:3, n = '3' RETURN NONE;
ALTER FIELD n ON book TYPE int MIGRATE EXISTING ROWS AS n_int USING <int>$value FALLBACK 0;
(INFO FOR TABLE book).migrations.generation;
(INFO FOR TABLE book).migrations.guaranteed;
-- Added.
ALTER FIELD by ON book REFERENCE ON DELETE REJECT;
(INFO FOR TABLE book).fields.by;
(INFO FOR TABLE book).migrations.generation;
(INFO FOR TABLE book).migrations.guaranteed;
SELECT id, by, n, type::is_int(n) AS ni FROM book ORDER BY id;
-- The rows stored before it have no backlink, and a write that leaves their
-- value as it was adds none.
SELECT id, <~book AS b FROM author ORDER BY id;
UPDATE book:2 SET title = 't' RETURN NONE;
UPDATE book:2 SET by = author:2 RETURN NONE;
SELECT id, <~book AS b FROM author ORDER BY id;
-- So an ON DELETE action does not see them.
DELETE author:1;
SELECT id, by FROM book ORDER BY id;
-- A row written afterwards has its backlink, and its action.
CREATE book:4 SET by = author:2, n = 4 RETURN NONE;
DELETE author:2;
SELECT id, <~book AS b FROM author ORDER BY id;
-- An old row whose value changes gets one.
UPDATE book:3 SET by = author:4 RETURN NONE;
UPDATE book:3 SET by = author:3 RETURN NONE;
SELECT id, <~book AS b FROM author ORDER BY id;
-- Dropped: every backlink of the field goes, the values stay.
ALTER FIELD by ON book DROP REFERENCE;
SELECT id, <~book AS b FROM author ORDER BY id;
SELECT id, by FROM book ORDER BY id;
(INFO FOR TABLE book).fields.by;
(INFO FOR TABLE book).migrations.generation;
(INFO FOR TABLE book).migrations.guaranteed;
-- Added again: nothing is restored for the rows that have a value.
ALTER FIELD by ON book REFERENCE;
SELECT id, <~book AS b FROM author ORDER BY id;
CREATE book:5 SET by = author:3, n = 5 RETURN NONE;
SELECT id, <~book AS b FROM author ORDER BY id;
(INFO FOR TABLE book).migrations.generation;
-- An enforced table with no rows indexes everything written after it.
DEFINE TABLE page SCHEMAFULL;
DEFINE FIELD of ON page TYPE option<record<author>>;
ALTER FIELD of ON page REFERENCE;
CREATE page:1 SET of = author:3 RETURN NONE;
SELECT id, <~page AS b FROM author:3;
(INFO FOR TABLE page).migrations.generation;
-- A SCHEMALESS table and a database at compatibility level 3 behave alike.
DEFINE TABLE loose SCHEMALESS;
DEFINE FIELD by ON loose TYPE option<record<author>>;
CREATE loose:1 SET by = author:3 RETURN NONE;
ALTER FIELD by ON loose REFERENCE;
SELECT id, <~loose AS b FROM author:3;
UPDATE loose:1 SET by = author:3, title = 'x' RETURN NONE;
SELECT id, <~loose AS b FROM author:3;
DEFINE DATABASE d3 COMPATIBILITY 3;
USE DB d3;
DEFINE TABLE author SCHEMAFULL;
DEFINE TABLE book SCHEMAFULL;
DEFINE FIELD by ON book TYPE option<record<author>>;
CREATE author:1, author:2 RETURN NONE;
CREATE book:1 SET by = author:1 RETURN NONE;
ALTER FIELD by ON book REFERENCE;
SELECT id, <~book AS b FROM author ORDER BY id;
CREATE book:2 SET by = author:2 RETURN NONE;
UPDATE book:1 SET by = author:2 RETURN NONE;
SELECT id, <~book AS b FROM author ORDER BY id;
ALTER FIELD by ON book DROP REFERENCE;
SELECT id, <~book AS b FROM author ORDER BY id;
SELECT id, by FROM book ORDER BY id;
