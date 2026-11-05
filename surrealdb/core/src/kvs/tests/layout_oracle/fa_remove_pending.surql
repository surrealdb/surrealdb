-- A field removed while rows are still behind a migration step on it. The pending step must not
-- reach a field of the same name defined later: a re-declared field of another type with no MIGRATE
-- starts empty, rows written in the gap hold what they wrote, a write to the removed name is refused
-- while the table is enforced, and a migration name that already ran on the field stays taken.
DEFINE TABLE p SCHEMAFULL;
DEFINE FIELD a ON p TYPE string;
DEFINE FIELD b ON p TYPE option<int>;
CREATE p:1 SET a = '5', b = 1 RETURN NONE;
CREATE p:2 SET a = 'x' RETURN NONE;
-- @upgrade
ALTER FIELD a ON p TYPE int MIGRATE EXISTING ROWS AS a_int USING <int>$value FALLBACK 0;
-- Rows are behind a_int in lazy mode. Remove the field before the job reaches them.
REMOVE FIELD a ON p;
SELECT * FROM p ORDER BY id;
CREATE p:3 SET a = 'gone', b = 3 RETURN NONE;
CREATE p:3 SET b = 3 RETURN NONE;
SELECT * FROM p ORDER BY id;
-- Re-declared with another type and no MIGRATE.
DEFINE FIELD a ON p TYPE option<string>;
SELECT * FROM p ORDER BY id;
CREATE p:4 SET a = 'four', b = 4 RETURN NONE;
SELECT * FROM p ORDER BY id;
-- @check
-- @drain
SELECT * FROM p ORDER BY id;
SELECT VALUE id FROM p WHERE a = 'x' ORDER BY id;
SELECT VALUE id FROM p WHERE a = 5 ORDER BY id;
(INFO FOR TABLE p).migrations;
-- Refused: a removed field re-declared non-optional, with rows and no MIGRATE. The refusal leaves
-- the field undeclared, so the name is still free for the next definition.
REMOVE FIELD a ON p;
DEFINE FIELD a ON p TYPE int;
(INFO FOR TABLE p).fields;
SELECT * FROM p ORDER BY id;
DEFINE FIELD a ON p TYPE int MIGRATE EXISTING ROWS AS a_int USING <int>$value FALLBACK 9;
DEFINE FIELD a ON p TYPE int MIGRATE EXISTING ROWS AS a_back USING <int>$value FALLBACK 9;
SELECT * FROM p ORDER BY id;
-- @check
-- @drain
SELECT * FROM p ORDER BY id;
(INFO FOR TABLE p).migrations;
-- A migration on b is minted, then b is removed while its step is pending, then b is re-declared
-- as a string and written to.
ALTER FIELD b ON p TYPE int MIGRATE EXISTING ROWS AS b_fill USING <int>$value FALLBACK 0;
REMOVE FIELD b ON p;
SELECT * FROM p ORDER BY id;
DEFINE FIELD b ON p TYPE option<string>;
UPDATE p:2 SET b = 'two' RETURN NONE;
SELECT * FROM p ORDER BY id;
-- @check
-- @drain
SELECT * FROM p ORDER BY id;
