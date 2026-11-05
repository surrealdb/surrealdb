-- Field removal and re-declaration on a SCHEMAFULL table, first in the scenario's own database and
-- then in one altered to compatibility level 3, where no table is enforced. On an enforced table a
-- removal clears the values for every reader; at level 3 the stored values stay put. Either way a
-- removed name is re-declared and written to, a MIGRATE at level 3 fills the rows as it does at level
-- 4, and the level-3 database must answer the same after an upgrade of the store.
DEFINE TABLE p SCHEMAFULL;
DEFINE FIELD a ON p TYPE int;
DEFINE FIELD c ON p TYPE int;
CREATE p:1 SET a = 1, c = 3 RETURN NONE;
CREATE p:2 SET a = 2, c = 2 RETURN NONE;
-- @upgrade
REMOVE FIELD c ON p;
SELECT * FROM p ORDER BY id;
CREATE p:3 SET a = 3, c = 'x' RETURN NONE;
DEFINE FIELD c ON p TYPE option<string>;
CREATE p:3 SET a = 3, c = 'x' RETURN NONE;
SELECT * FROM p ORDER BY id;
-- @check
-- @drain
SELECT * FROM p ORDER BY id;
(INFO FOR TABLE p).migrations;
-- The same sequence at compatibility level 3, in its own database.
DEFINE DATABASE d3 COMPATIBILITY 3;
USE DB d3;
DEFINE TABLE q SCHEMAFULL;
DEFINE FIELD a ON q TYPE int;
DEFINE FIELD c ON q TYPE int;
CREATE q:1 SET a = 1, c = 3 RETURN NONE;
CREATE q:2 SET a = 2, c = 2 RETURN NONE;
REMOVE FIELD c ON q;
SELECT * FROM q ORDER BY id;
DEFINE FIELD c ON q TYPE option<string>;
SELECT * FROM q ORDER BY id;
CREATE q:3 SET a = 3, c = 'three' RETURN NONE;
SELECT * FROM q ORDER BY id;
-- A MIGRATE at level 3 fills the rows it names.
DEFINE FIELD k ON q TYPE int MIGRATE EXISTING ROWS AS k_fill USING 5;
SELECT * FROM q ORDER BY id;
(INFO FOR TABLE q).migrations;
-- @check
REMOVE FIELD k ON q;
SELECT * FROM q ORDER BY id;
-- At level 3 the removed name is still stored on the rows: a write to such a row is checked against
-- it, and only an UNSET of the stored name or a re-declaration can clear it.
UPDATE q:3 SET a = 9 RETURN NONE;
UPDATE q:3 UNSET k RETURN NONE;
SELECT * FROM q ORDER BY id;
DEFINE FIELD k ON q TYPE option<int>;
CREATE q:4 SET a = 4, k = 'k4' RETURN NONE;
SELECT * FROM q ORDER BY id;
-- @drain
SELECT * FROM q ORDER BY id;
(INFO FOR TABLE q).fields;
-- @check
