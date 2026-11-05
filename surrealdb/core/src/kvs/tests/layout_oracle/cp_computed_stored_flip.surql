-- A stored field guaranteed by a migration, made COMPUTED, stored again, and made COMPUTED again. A guarantee holds
-- only while the field is stored; a COMPUTED field reading it answers what the field holds at that moment; a value
-- removed when the field became COMPUTED does not come back when it is stored again; a TYPE that refuses NONE is
-- refused over rows that hold nothing for the field; and a later migration reaches only the values the rows hold.
DEFINE TABLE fl SCHEMAFULL;
DEFINE FIELD q ON fl TYPE string;
DEFINE FIELD c ON fl TYPE option<string>;
CREATE fl:1 SET q = '1', c = '10' RETURN NONE;
CREATE fl:2 SET q = '2', c = 'x' RETURN NONE;
CREATE fl:3 SET q = '3' RETURN NONE;
-- @upgrade
ALTER FIELD q ON fl TYPE int MIGRATE EXISTING ROWS AS q_int USING <int>$value FALLBACK 0;
ALTER FIELD c ON fl TYPE option<int> MIGRATE EXISTING ROWS AS c_int USING <int>$value FALLBACK -1;
DEFINE FIELD d ON fl COMPUTED (q ?? 0) + (c ?? 0);
SELECT * FROM fl ORDER BY id;
-- @check
-- @drain
(INFO FOR TABLE fl).migrations.guaranteed;
SELECT * FROM fl ORDER BY id;
-- The stored c becomes COMPUTED: its values are removed for every reader and the guarantee goes with them.
DEFINE FIELD OVERWRITE c ON fl COMPUTED (q ?? 0) * 2;
(INFO FOR TABLE fl).migrations.guaranteed;
SELECT * FROM fl ORDER BY id;
UPDATE fl:3 SET q = 30 RETURN NONE;
SELECT * FROM fl ORDER BY id;
-- @check
-- Stored again as option<int>: the rows hold nothing for c, not the values it held before it was computed.
DEFINE FIELD OVERWRITE c ON fl TYPE option<int>;
(INFO FOR TABLE fl).migrations;
SELECT * FROM fl ORDER BY id;
UPDATE fl:2 SET c = 22 RETURN NONE;
SELECT * FROM fl ORDER BY id;
-- @check
-- @drain
(INFO FOR TABLE fl).migrations.guaranteed;
SELECT * FROM fl ORDER BY id;
-- A TYPE that refuses NONE cannot be stored over rows that hold nothing for c.
DEFINE FIELD OVERWRITE c ON fl TYPE int;
SELECT * FROM fl ORDER BY id;
-- A migration over the re-stored field, with a fallback for the rows that hold nothing.
ALTER FIELD c ON fl TYPE int MIGRATE EXISTING ROWS AS c_fill USING <int>$value FALLBACK 7;
SELECT * FROM fl ORDER BY id;
(INFO FOR TABLE fl).migrations;
-- @check
-- @drain
(INFO FOR TABLE fl).migrations.guaranteed;
SELECT * FROM fl ORDER BY id;
-- Made COMPUTED again: the stored values are removed and the guarantee goes.
DEFINE FIELD OVERWRITE c ON fl COMPUTED (q ?? 0) + 100;
(INFO FOR TABLE fl).migrations.guaranteed;
SELECT * FROM fl ORDER BY id;
-- @check
