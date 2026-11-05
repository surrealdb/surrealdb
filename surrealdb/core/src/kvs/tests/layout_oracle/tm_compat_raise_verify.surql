-- A field re-typed without MIGRATE at compatibility level 3 over rows the new type refuses: level 3
-- accepts the definition and the rows as stored; a raise to level 4 verifies the field, reports each
-- row that fails it without rewriting any of them, and claims no guarantee for the field meanwhile;
-- level 4 writes refuse what the field does not take; a lowering and a raise again verify it afresh;
-- and a migration minted at level 4 replays over the rows as they are.
DEFINE TABLE cv SCHEMAFULL;
DEFINE FIELD n ON cv TYPE string;
CREATE cv:1 SET n = '1' RETURN NONE;
CREATE cv:2 SET n = 'two' RETURN NONE;
CREATE cv:3 SET n = '3' RETURN NONE;
-- @upgrade
ALTER DATABASE test COMPATIBILITY 3;
ALTER FIELD n ON cv TYPE int;
SELECT * FROM cv ORDER BY id;
(INFO FOR TABLE cv).migrations;
-- @drain
SELECT * FROM cv ORDER BY id;
-- The raise: every row fails the field. Each is reported by the verification, and none is rewritten.
ALTER DATABASE test COMPATIBILITY 4;
(INFO FOR DB).compatibility;
SELECT * FROM cv ORDER BY id;
(INFO FOR TABLE cv).migrations;
CREATE cv:4 SET n = 'four' RETURN NONE;
CREATE cv:5 SET n = 5 RETURN NONE;
UPDATE cv:1 SET n = 1 RETURN AFTER;
SELECT * FROM cv ORDER BY id;
(INFO FOR TABLE cv).migrations;
-- @check
-- Lowered and raised again: the verification runs afresh and the rows read the same.
ALTER DATABASE test COMPATIBILITY 3;
CREATE cv:6 SET n = 'six' RETURN NONE;
SELECT * FROM cv ORDER BY id;
ALTER DATABASE test COMPATIBILITY 4;
(INFO FOR TABLE cv).migrations;
SELECT * FROM cv ORDER BY id;
-- @drain
SELECT * FROM cv ORDER BY id;
(INFO FOR TABLE cv).migrations;
-- A migration minted at level 4 replays over the rows as they are: only the rows that do not hold an
-- int take the fallback.
ALTER FIELD n ON cv TYPE int MIGRATE EXISTING ROWS AS n_int2 USING <int>$value FALLBACK -2;
SELECT * FROM cv ORDER BY id;
(INFO FOR TABLE cv).migrations.migrations;
-- @check
-- @drain
SELECT * FROM cv ORDER BY id;
(INFO FOR TABLE cv).migrations;
-- @check
