-- A REFERENCE added to a field while a migration that turns its strings into
-- record links is still pending: the links the migration writes are what an
-- ON DELETE action follows once the job has reached a row, and until then the
-- backlinks of the old strings are absent. Migrating a REFERENCE field itself
-- is refused and leaves the links as they were. Every mode must agree on which
-- books a DELETE takes with it.
DEFINE TABLE author SCHEMAFULL;
DEFINE TABLE book SCHEMAFULL;
DEFINE FIELD by ON book TYPE option<string>;
DEFINE FIELD title ON book TYPE option<string>;
CREATE author:1, author:2, author:3 RETURN NONE;
CREATE book:1 SET by = 'author:1' RETURN NONE;
CREATE book:2 SET by = 'author:2' RETURN NONE;
CREATE book:3 SET by = 'author:3', title = 't3' RETURN NONE;
CREATE book:4 SET title = 'none' RETURN NONE;
-- @upgrade
ALTER FIELD by ON book TYPE option<record<author>> MIGRATE EXISTING ROWS AS by_rec USING <record<author>>$value FALLBACK NONE;
ALTER FIELD by ON book REFERENCE ON DELETE CASCADE;
SELECT id, by FROM book ORDER BY id;
SELECT id, <~book AS b FROM author ORDER BY id;
-- @check
-- @drain
SELECT id, by FROM book ORDER BY id;
SELECT id, <~book AS b FROM author ORDER BY id;
-- The migration of a REFERENCE field is refused.
ALTER FIELD by ON book MIGRATE EXISTING ROWS AS again USING $value FALLBACK NONE;
SELECT id, by FROM book ORDER BY id;
-- A DELETE of a target takes the books that link to it.
DELETE author:1;
SELECT id, by FROM book ORDER BY id;
SELECT id, <~book AS b FROM author ORDER BY id;
-- A row written after the REFERENCE has its backlink.
CREATE book:5 SET by = author:2, title = 't5' RETURN NONE;
SELECT id, <~book AS b FROM author ORDER BY id;
DELETE author:2;
SELECT id, by FROM book ORDER BY id;
-- Re-pointing an old row moves its backlink.
UPDATE book:3 SET by = author:3 RETURN NONE;
SELECT id, <~book AS b FROM author ORDER BY id;
-- @check
-- Dropping the REFERENCE purges its backlinks and keeps the links.
ALTER FIELD by ON book DROP REFERENCE;
SELECT id, <~book AS b FROM author ORDER BY id;
SELECT id, by FROM book ORDER BY id;
(INFO FOR TABLE book).fields.by;
(INFO FOR TABLE book).migrations.generation;
(INFO FOR TABLE book).migrations.guaranteed;
-- Added again: no backlink is restored for the links that exist.
ALTER FIELD by ON book REFERENCE ON DELETE REJECT;
SELECT id, <~book AS b FROM author ORDER BY id;
DELETE author:3;
SELECT id, by FROM book ORDER BY id;
-- @check
-- A REFERENCE on a field whose TYPE holds no record: DEFINE refuses it, and the
-- ALTER form of the same clause is judged by the same rule. The clause is then
-- dropped again so that the export carries nothing this check left behind.
DEFINE FIELD num ON book TYPE option<int>;
DEFINE FIELD numr ON book TYPE option<int> REFERENCE;
ALTER FIELD num ON book REFERENCE;
(INFO FOR TABLE book).fields.num;
DEFINE FIELD OVERWRITE num ON book TYPE option<int>;
(INFO FOR TABLE book).fields.num;
SELECT id, by FROM book ORDER BY id;
-- @check
