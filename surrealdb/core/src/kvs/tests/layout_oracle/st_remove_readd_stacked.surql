-- Removal steps stacked with rewrite steps before one job run. A field migrated
-- and then removed before the job reaches its rows loses the value the rewrite
-- would have produced; a field defined again afterwards starts from the rows as
-- the removal left them, and a restatement of the rewrite's name under the new
-- definition is judged against the step that ran. An index that reads a field
-- forbids removing that field until the index is gone.
DEFINE TABLE rr SCHEMAFULL;
DEFINE FIELD a ON rr TYPE string;
DEFINE FIELD b ON rr TYPE string;
DEFINE FIELD c ON rr TYPE string;
DEFINE INDEX by_c ON rr FIELDS c;
CREATE rr:1 SET a = '1', b = 'x', c = 'p' RETURN NONE;
CREATE rr:2 SET a = '2', b = 'y', c = 'q' RETURN NONE;
-- @upgrade
ALTER FIELD a ON rr TYPE int MIGRATE EXISTING ROWS AS a_int USING <int>$value FALLBACK 0;
REMOVE FIELD a ON rr;
SELECT * FROM rr ORDER BY id;
DEFINE FIELD a ON rr TYPE option<float>;
SELECT * FROM rr ORDER BY id;
ALTER FIELD a ON rr TYPE int MIGRATE EXISTING ROWS AS a_int USING <int>$value FALLBACK 0;
SELECT * FROM rr ORDER BY id;
(INFO FOR TABLE rr).fields;
(INFO FOR TABLE rr).migrations;
-- @check
-- A rewrite of b queued and removed in the same batch; c is removed only after
-- its index is dropped.
ALTER FIELD b ON rr TYPE int MIGRATE EXISTING ROWS AS b_int USING <int>$value FALLBACK 0;
REMOVE FIELD b ON rr;
DEFINE FIELD b ON rr TYPE option<string>;
SELECT * FROM rr ORDER BY id;
REMOVE FIELD c ON rr;
REMOVE INDEX by_c ON rr;
REMOVE FIELD c ON rr;
SELECT * FROM rr ORDER BY id;
(INFO FOR TABLE rr).migrations;
-- @drain
SELECT * FROM rr ORDER BY id;
(INFO FOR TABLE rr).migrations;
-- @check
-- A removal of a field the job has already rewritten, then a field of the same
-- name with a migration of its own.
REMOVE FIELD a ON rr;
DEFINE FIELD a ON rr TYPE option<int>;
ALTER FIELD a ON rr TYPE int MIGRATE EXISTING ROWS AS a_again USING <int>$value FALLBACK 5;
SELECT * FROM rr ORDER BY id;
-- @drain
SELECT * FROM rr ORDER BY id;
-- @check
CREATE rr:3 SET a = 3, b = 'z' RETURN NONE;
SELECT * FROM rr ORDER BY id;
-- @drain
-- @check
