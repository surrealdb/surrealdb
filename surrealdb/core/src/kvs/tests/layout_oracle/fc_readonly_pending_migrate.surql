-- READONLY on a field a migration changes while the job has not reached the
-- rows: a write that restates an old row's migrated value changes nothing and
-- is accepted, and a write that moves it is refused, whether the row is still
-- behind its step or not. Lazy and eager runs must judge the same write alike.
DEFINE TABLE p SCHEMAFULL;
DEFINE FIELD f ON p TYPE string;
DEFINE FIELD g ON p TYPE option<int>;
CREATE p:1 SET f = '1', g = 1 RETURN NONE;
CREATE p:2 SET f = '2', g = 2 RETURN NONE;
CREATE p:3 SET f = 'x', g = 3 RETURN NONE;
-- @upgrade
ALTER FIELD f ON p TYPE int MIGRATE EXISTING ROWS AS f_int USING <int>$value FALLBACK -1;
ALTER FIELD f ON p READONLY;
SELECT * FROM p ORDER BY id;
-- Restating the migrated value of an old row: no change, so accepted.
UPDATE p:1 SET f = 1 RETURN NONE;
-- Moving it: refused.
UPDATE p:2 SET f = 9 RETURN NONE;
-- A write to another field leaves a fallback row's value alone.
UPDATE p:3 SET g = 30 RETURN NONE;
SELECT * FROM p ORDER BY id;
-- Restating the fallback value: accepted; moving it: refused.
UPDATE p:3 SET f = -1 RETURN NONE;
UPDATE p:3 SET f = 0 RETURN NONE;
SELECT * FROM p ORDER BY id;
-- @check
-- @drain
SELECT * FROM p ORDER BY id;
UPDATE p:1 SET f = 1 RETURN NONE;
UPDATE p:2 SET f = 2 RETURN NONE;
UPDATE p:2 SET f = 9 RETURN NONE;
SELECT * FROM p ORDER BY id;
-- DEFAULT ALWAYS with READONLY on a field whose old rows hold other values: the
-- default reapplies on every write, so a write that does not mention g changes
-- an old row's g and is refused as a change to a readonly field, or not.
ALTER FIELD g ON p DEFAULT ALWAYS 5 READONLY;
SELECT * FROM p ORDER BY id;
UPDATE p:1 SET f = 1 RETURN NONE;
SELECT * FROM p ORDER BY id;
-- A new row takes the default and cannot have it changed.
CREATE p:4 SET f = '4' RETURN NONE;
UPDATE p:4 SET g = 6 RETURN NONE;
UPDATE p:4 SET f = 4 RETURN NONE;
SELECT * FROM p ORDER BY id;
-- Dropping READONLY releases f, and the default of g stays.
ALTER FIELD f ON p DROP READONLY;
UPDATE p:2 SET f = 9 RETURN NONE;
ALTER FIELD g ON p DROP READONLY;
UPDATE p:4 SET g = 6 RETURN NONE;
SELECT * FROM p ORDER BY id;
-- @check
(INFO FOR TABLE p).fields;
(INFO FOR TABLE p).migrations.generation;
(INFO FOR TABLE p).migrations.guaranteed;
