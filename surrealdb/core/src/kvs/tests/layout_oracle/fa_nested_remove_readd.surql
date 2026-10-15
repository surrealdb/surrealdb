-- An object field with declared fields beneath it, removed and re-declared. The removed subtree must
-- not reappear when the object is declared again, a re-declared child starts empty, a write to a
-- removed path is refused while the table is enforced, and a child removed and re-declared with a
-- different TYPE and MIGRATE reads only what its new definition makes of the rows.
DEFINE TABLE u SCHEMAFULL;
DEFINE FIELD prof ON u TYPE option<object>;
DEFINE FIELD prof.name ON u TYPE option<string>;
DEFINE FIELD prof.age ON u TYPE option<int>;
CREATE u:1 SET prof = { name: 'a', age: 3 } RETURN NONE;
CREATE u:2 SET prof = { name: 'b' } RETURN NONE;
-- @upgrade
REMOVE FIELD prof ON u;
SELECT * FROM u ORDER BY id;
(INFO FOR TABLE u).fields;
CREATE u:3 SET prof = { name: 'c' } RETURN NONE;
DEFINE FIELD prof ON u TYPE option<object> FLEXIBLE;
SELECT * FROM u ORDER BY id;
CREATE u:3 SET prof = { name: 'c', extra: 1 } RETURN NONE;
SELECT * FROM u ORDER BY id;
-- The child declared again: the age stored before the removal must not come back.
DEFINE FIELD prof.age ON u TYPE option<int>;
SELECT * FROM u ORDER BY id;
SELECT VALUE id FROM u WHERE prof.age = 3;
-- @check
-- Removing one child leaves its sibling and the parent.
DEFINE FIELD prof.name ON u TYPE option<string>;
UPDATE u:3 SET prof = { name: 'c', age: 4, extra: 1 } RETURN NONE;
REMOVE FIELD prof.name ON u;
SELECT * FROM u ORDER BY id;
(INFO FOR TABLE u).fields;
-- Re-declared as an int that every old row is given a value for.
DEFINE FIELD prof.name ON u TYPE int MIGRATE EXISTING ROWS AS name_int USING 0;
SELECT * FROM u ORDER BY id;
-- @drain
SELECT * FROM u ORDER BY id;
SELECT VALUE id FROM u WHERE prof.name = 0 ORDER BY id;
(INFO FOR TABLE u).migrations;
-- @check
-- A removal and re-declaration of the whole object in one transaction, then the child written.
-- @client
REMOVE FIELD prof ON u;
DEFINE FIELD prof ON u TYPE option<object> FLEXIBLE;
UPDATE u:2 SET prof = { name: 'two' } RETURN NONE;
-- @end
SELECT * FROM u ORDER BY id;
-- @check
-- @drain
SELECT * FROM u ORDER BY id;
