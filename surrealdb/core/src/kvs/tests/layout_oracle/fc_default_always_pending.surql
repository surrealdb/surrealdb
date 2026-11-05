-- A DEFAULT ALWAYS set with a TYPE change under MIGRATE: the migration applies
-- neither the default nor anything else to a row whose value is absent, so an
-- absent row takes the FALLBACK; a later write of an old row reapplies DEFAULT
-- ALWAYS over the migrated value; a plain DEFAULT added afterwards fills rows
-- created after it and never a value already stored; dropping the default
-- leaves stored values alone.
DEFINE TABLE t SCHEMAFULL;
DEFINE FIELD a ON t TYPE option<string>;
DEFINE FIELD k ON t TYPE int;
DEFINE FIELD note ON t TYPE option<string>;
CREATE t:1 SET a = '1', k = 1, note = 'old' RETURN NONE;
CREATE t:2 SET a = 'x', k = 2 RETURN NONE;
CREATE t:3 SET k = 3 RETURN NONE;
CREATE t:4 SET a = '4', k = 4, note = 'four' RETURN NONE;
-- @upgrade
ALTER FIELD a ON t TYPE option<int> DEFAULT ALWAYS 7 MIGRATE EXISTING ROWS AS a_int USING <int>$value FALLBACK -1;
(INFO FOR TABLE t).migrations.generation;
(INFO FOR TABLE t).migrations.guaranteed;
SELECT * FROM t ORDER BY id;
-- A write of an old row that leaves a alone reapplies DEFAULT ALWAYS.
UPDATE t:1 SET k = 10 RETURN NONE;
SELECT id, a, k FROM t:1;
-- A plain DEFAULT added now: no stored value changes, and a created row takes it.
ALTER FIELD note ON t DEFAULT 'later';
SELECT * FROM t ORDER BY id;
CREATE t:5 SET a = '5', k = 5 RETURN NONE;
SELECT * FROM t:5;
-- An old row without a note is not given the default by an update.
UPDATE t:3 SET k = 30 RETURN NONE;
SELECT id, note, k FROM t:3;
-- @check
-- @drain
SELECT * FROM t ORDER BY id;
(INFO FOR TABLE t).migrations.generation;
-- Dropping the default: the stored values stay, the next write does not reapply it.
ALTER FIELD a ON t DROP DEFAULT;
UPDATE t:1 SET k = 11 RETURN NONE;
UPDATE t:2 SET k = 22 RETURN NONE;
SELECT * FROM t ORDER BY id;
-- A value written under a default written back explicitly sticks.
UPDATE t:2 SET a = NONE RETURN NONE;
UPDATE t:4 SET a = 44 RETURN NONE;
SELECT id, a, k FROM t ORDER BY id;
-- Re-adding DEFAULT ALWAYS with a value the old rows already hold.
ALTER FIELD a ON t DEFAULT ALWAYS 7;
UPDATE t:4 SET k = 40 RETURN NONE;
UPDATE t:2 SET k = 23 RETURN NONE;
SELECT id, a, k FROM t ORDER BY id;
(INFO FOR TABLE t).fields;
(INFO FOR TABLE t).migrations.generation;
-- @check
SELECT * FROM t ORDER BY id;
