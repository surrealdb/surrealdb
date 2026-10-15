-- ALTER DATABASE COMPATIBILITY 4 after the upgrade, on a table whose legacy rows already break
-- their declared TYPE and hold a field that was removed at level 3, where REMOVE kept the
-- values. The raise queues a verification and rewrites nothing: reads keep returning what the
-- rows hold, the verification reports what it finds, a new write must satisfy the level-4 rules,
-- and a migration over the mistyped field brings its rows to the new type.
DEFINE TABLE book SCHEMAFULL;
DEFINE FIELD title ON book TYPE string;
DEFINE FIELD pages ON book TYPE int;
DEFINE FIELD isbn ON book TYPE option<string>;
DEFINE FIELD rating ON book TYPE option<int>;
DEFINE FIELD code ON book TYPE string;
CREATE book:1 SET title = 'A', pages = 100, isbn = '1', rating = 5, code = 'x1';
CREATE book:2 SET title = 'B', pages = 200, isbn = '2', rating = 3, code = 'x2';
CREATE book:3 SET title = 'C', pages = 300, code = 'x3';
ALTER FIELD pages ON book TYPE string;
CREATE book:4 SET title = 'D', pages = 'four hundred', code = 'x4';
ALTER FIELD rating ON book TYPE option<string>;
REMOVE FIELD code ON book;
-- @upgrade
SELECT * FROM book ORDER BY id;
ALTER DATABASE test COMPATIBILITY 4;
SELECT * FROM book ORDER BY id;
(INFO FOR TABLE book).migrations;
SELECT VALUE id FROM book WHERE pages = 'four hundred';
-- @drain
SELECT * FROM book ORDER BY id;
(INFO FOR TABLE book).migrations;
-- @check
CREATE book:5 SET title = 'E', pages = 5;
CREATE book:6 SET title = 'F', pages = 'many';
UPDATE book:1 SET title = 'A2' RETURN AFTER;
UPDATE book:2 SET title = 7;
SELECT * FROM book ORDER BY id;
ALTER FIELD pages ON book TYPE string MIGRATE EXISTING ROWS AS pages_s USING <string>$value FALLBACK 'unknown';
-- @drain
SELECT * FROM book ORDER BY id;
(INFO FOR TABLE book).migrations;
CREATE book:7 SET title = 'G', pages = 'x', rating = 5;
SELECT VALUE id FROM book WHERE rating = 5 ORDER BY id;
-- @check
DEFINE FIELD code ON book TYPE option<string>;
SELECT * FROM book ORDER BY id;
UPDATE book:3 SET code = 'y3' RETURN AFTER;
SELECT VALUE id FROM book WHERE code = 'x1' ORDER BY id;
-- @check
