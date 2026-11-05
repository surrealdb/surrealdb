-- COMPUTED fields over rows with stored values: a computed field reads the value
-- a pending migration gives its input, a computed field chained on another
-- computed field reads through it, and a COMPUTED field made stored, or a
-- stored one made COMPUTED, leaves every old row answering what its definition
-- now says and nothing it no longer stores. Lazy and eager runs answer alike.
DEFINE TABLE p SCHEMAFULL;
DEFINE FIELD a ON p TYPE option<int>;
DEFINE FIELD b ON p TYPE option<int>;
CREATE p:1 SET a = 1, b = 1 RETURN NONE;
CREATE p:2 SET a = 2, b = 2 RETURN NONE;
CREATE p:3 SET b = 3 RETURN NONE;
-- @upgrade
DEFINE FIELD c ON p COMPUTED (a ?? 0) * 2;
DEFINE FIELD d ON p COMPUTED (c ?? 0) + (b ?? 0);
SELECT * FROM p ORDER BY id;
-- A MIGRATE that keeps the TYPE rewrites every old a; a absent takes the fallback.
ALTER FIELD a ON p MIGRATE EXISTING ROWS AS a_plus USING ($value ?? 0) + 10 FALLBACK 0;
SELECT * FROM p ORDER BY id;
SELECT VALUE id FROM p WHERE c > 15 ORDER BY id;
DEFINE INDEX by_d ON p FIELDS d;
SELECT VALUE id FROM p WHERE d >= 20 ORDER BY id;
-- @check
-- @drain
SELECT * FROM p ORDER BY id;
-- A write to a stored input moves both computed fields.
UPDATE p:1 SET b = 5 RETURN NONE;
SELECT * FROM p ORDER BY id;
-- A COMPUTED field made stored: old rows hold nothing for it, and the field
-- beneath it reads the stored value.
DEFINE FIELD OVERWRITE c ON p TYPE option<int>;
SELECT * FROM p ORDER BY id;
UPDATE p:2 SET a = 7 RETURN NONE;
SELECT * FROM p ORDER BY id;
-- Made COMPUTED again: its stored values are removed for every reader.
DEFINE FIELD OVERWRITE c ON p COMPUTED (a ?? 0) * 3;
SELECT * FROM p ORDER BY id;
SELECT VALUE id FROM p WHERE d > 20 ORDER BY id;
-- @check
-- A stored input removed: the computed field over it reads what the removal
-- leaves, for every row.
REMOVE FIELD b ON p;
SELECT * FROM p ORDER BY id;
-- @drain
SELECT * FROM p ORDER BY id;
DEFINE FIELD b ON p TYPE option<int>;
UPDATE p:3 SET b = 30 RETURN NONE;
SELECT * FROM p ORDER BY id;
-- @check
(INFO FOR TABLE p).fields;
(INFO FOR TABLE p).migrations.generation;
(INFO FOR TABLE p).migrations.guaranteed;
