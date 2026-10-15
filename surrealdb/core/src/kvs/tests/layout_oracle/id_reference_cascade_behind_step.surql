-- A REFERENCE field added after its rows were written. A row holds a link only through a migration step until
-- the job reaches it, so whether a delete of the target cascades to that row must not depend on whether the
-- job has run. Rows written after the REFERENCE cascade; rows stored before it are not backfilled.
DEFINE TABLE person SCHEMAFULL;
DEFINE FIELD name ON person TYPE string;
DEFINE TABLE book SCHEMAFULL;
DEFINE FIELD title ON book TYPE string;
DEFINE FIELD by ON book TYPE option<string>;
CREATE person:1 SET name = 'Ann' RETURN NONE;
CREATE person:2 SET name = 'Bob' RETURN NONE;
CREATE book:1 SET title = 'A', by = 'person:1' RETURN NONE;
CREATE book:2 SET title = 'B', by = 'person:2' RETURN NONE;
CREATE book:3 SET title = 'C', by = 'person:1' RETURN NONE;
CREATE book:4 SET title = 'D', by = 'person:9' RETURN NONE;
CREATE book:5 SET title = 'E' RETURN NONE;
-- @upgrade
ALTER FIELD by ON book TYPE option<record<person>> MIGRATE EXISTING ROWS AS by_rec USING <record<person>>$value FALLBACK NONE;
ALTER FIELD by ON book REFERENCE ON DELETE CASCADE;
CREATE book:6 SET title = 'F', by = person:2 RETURN NONE;
SELECT VALUE title FROM book ORDER BY id;
SELECT VALUE by.name FROM book ORDER BY id;
-- @check
DELETE person:2 RETURN BEFORE;
SELECT VALUE title FROM book ORDER BY id;
-- A row stored before the REFERENCE, written by a user after it.
UPDATE book:3 SET title = 'C2' RETURN AFTER;
SELECT VALUE by FROM book ORDER BY id;
-- @drain
SELECT VALUE title FROM book ORDER BY id;
SELECT VALUE by FROM book ORDER BY id;
DELETE person:1;
SELECT VALUE title FROM book ORDER BY id;
-- @check
