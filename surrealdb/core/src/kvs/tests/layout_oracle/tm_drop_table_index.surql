-- A table made DROP while its rows are behind a migration step and indexed: a create or an update
-- takes no stored effect, a delete is applied as the statement states, the rows stored before DROP
-- keep their steps and index entries, and a table made normal again answers the rows that were
-- stored, with no index entry lacking its row. A migration minted under DROP is kept by the table.
DEFINE TABLE dr SCHEMAFULL;
DEFINE FIELD a ON dr TYPE string;
CREATE dr:1 SET a = '5' RETURN NONE;
CREATE dr:2 SET a = '6' RETURN NONE;
CREATE dr:3 SET a = 'x' RETURN NONE;
-- @upgrade
ALTER FIELD a ON dr TYPE int MIGRATE EXISTING ROWS AS a_int USING <int>$value + 10 FALLBACK -1;
DEFINE INDEX dr_a ON dr FIELDS a;
SELECT VALUE id FROM dr WHERE a = 15 ORDER BY id;
SELECT VALUE id FROM dr WHERE a >= 0 ORDER BY id;
-- @check
DEFINE TABLE OVERWRITE dr DROP SCHEMAFULL;
CREATE dr:4 SET a = 40 RETURN NONE;
UPDATE dr:1 SET a = 25 RETURN NONE;
DELETE dr:2 RETURN NONE;
SELECT * FROM dr ORDER BY id;
SELECT VALUE id FROM dr WHERE a = 15 ORDER BY id;
SELECT VALUE id FROM dr WHERE a = 25 ORDER BY id;
DEFINE TABLE OVERWRITE dr SCHEMAFULL;
SELECT * FROM dr ORDER BY id;
SELECT VALUE id FROM dr WHERE a = 15 ORDER BY id;
SELECT VALUE id FROM dr WHERE a = 25 ORDER BY id;
SELECT VALUE id FROM dr WHERE a >= 0 ORDER BY id;
-- @drain
SELECT * FROM dr ORDER BY id;
SELECT VALUE id FROM dr WHERE a >= 0 ORDER BY id;
-- @check
-- A migration minted under DROP, with the index removed for it and defined again after.
DEFINE TABLE OVERWRITE dr DROP SCHEMAFULL;
REMOVE INDEX dr_a ON dr;
ALTER FIELD a ON dr TYPE int MIGRATE EXISTING ROWS AS a_int2 USING <int>$value * 2 FALLBACK -1;
CREATE dr:5 SET a = 50 RETURN NONE;
DEFINE INDEX dr_a ON dr FIELDS a;
DEFINE TABLE OVERWRITE dr SCHEMAFULL;
SELECT * FROM dr ORDER BY id;
SELECT VALUE id FROM dr WHERE a >= 0 ORDER BY id;
(INFO FOR TABLE dr).migrations.migrations;
-- @drain
SELECT * FROM dr ORDER BY id;
SELECT VALUE id FROM dr WHERE a >= 0 ORDER BY id;
-- @check
