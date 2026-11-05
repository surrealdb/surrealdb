-- A statement that fails inside an import block leaves the block's earlier writes as the import made
-- them, and the rows of an import that misfit their fields are stored as written and reached by a later
-- migration like any row behind a step. A statement that fails outside a transaction leaves nothing,
-- and one that fails in a caller-owned transaction after writing refuses that transaction's commit, so
-- it leaves nothing either.
DEFINE TABLE p SCHEMAFULL;
DEFINE FIELD a ON p TYPE string;
DEFINE FIELD b ON p TYPE string;
CREATE p:1 SET a = '1', b = '2' RETURN NONE;
ALTER FIELD a ON p TYPE int MIGRATE EXISTING ROWS AS a_int USING <int>$value FALLBACK 0;
BEGIN;
OPTION IMPORT;
INSERT INTO p [{ id: p:10, a: 'x', b: 'y' }, { id: p:1, a: 'dup', b: 'd' }, { id: p:11, a: 7, b: 'z' }];
CREATE p:12 SET a = 'w', b = 'v';
OPTION IMPORT = false;
COMMIT;
SELECT * FROM p ORDER BY id;
(INFO FOR TABLE p).migrations.guaranteed;
-- @check
-- A failed statement with no import: the INSERT writes its first row before the duplicate refuses it,
-- so the caller's commit is refused and no statement of the transaction keeps its write.
-- @client
CREATE p:20 SET a = 1, b = 'q';
INSERT INTO p [{ id: p:21, a: 2, b: 'r' }, { id: p:1, a: 9, b: 's' }];
CREATE p:22 SET a = 'bad', b = 't';
CREATE p:23 SET a = 3, b = 'u';
-- @end
SELECT * FROM p ORDER BY id;
-- @check
-- A migration over the other field reaches every row, including the rows the import wrote.
ALTER FIELD b ON p TYPE int MIGRATE EXISTING ROWS AS b_int USING <int>$value FALLBACK 0;
SELECT * FROM p ORDER BY id;
-- @drain
SELECT * FROM p ORDER BY id;
(INFO FOR TABLE p).migrations.guaranteed;
-- @check
