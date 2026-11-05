-- OPTION IMPORT rows that misfit a migrated field, read through COMPUTED fields. A misfit row forfeits the guarantee of
-- the field it misfits and leaves the other field's guarantee in force; a COMPUTED field reading the misfit value
-- answers what its expression yields for that row; a COMPUTED field whose expression yields NONE is judged the same way
-- by a scan and a filter; and the export of the table imports to the same rows.
DEFINE TABLE im SCHEMAFULL;
DEFINE FIELD a ON im TYPE string;
DEFINE FIELD b ON im TYPE string;
CREATE im:1 SET a = '1', b = '2' RETURN NONE;
CREATE im:2 SET a = '5', b = '6' RETURN NONE;
-- @upgrade
ALTER FIELD a ON im TYPE int MIGRATE EXISTING ROWS AS a_int USING <int>$value FALLBACK 0;
ALTER FIELD b ON im TYPE int MIGRATE EXISTING ROWS AS b_int USING <int>$value FALLBACK 0;
DEFINE FIELD twice ON im TYPE option<int> COMPUTED IF type::is_int(a) THEN a * 2 ELSE NONE END;
DEFINE FIELD strict ON im TYPE option<int> COMPUTED IF type::is_int(b) THEN b + 1 ELSE NONE END;
SELECT * FROM im ORDER BY id;
-- @check
-- @drain
(INFO FOR TABLE im).migrations.guaranteed;
-- A misfit in a only: b keeps its guarantee.
BEGIN;
OPTION IMPORT;
CREATE im:3 SET a = 'x', b = 7 RETURN NONE;
CREATE im:4 SET a = 4, b = 8 RETURN NONE;
OPTION IMPORT = false;
COMMIT;
(INFO FOR TABLE im).migrations.guaranteed;
SELECT * FROM im ORDER BY id;
SELECT VALUE id FROM im WHERE twice > 4 ORDER BY id;
SELECT VALUE id FROM im WHERE strict > 1 ORDER BY id;
-- @check
-- A second import misfits b on one row: a is still misfit on im:3, so neither field is guaranteed.
BEGIN;
OPTION IMPORT;
CREATE im:7 SET a = 7, b = 'z' RETURN NONE;
OPTION IMPORT = false;
COMMIT;
(INFO FOR TABLE im).migrations.guaranteed;
SELECT * FROM im ORDER BY id;
-- @check
-- A write that restates a field's TYPE is still checked against it.
CREATE im:5 SET a = '5', b = '9' RETURN NONE;
CREATE im:6 SET a = 'six', b = '6' RETURN NONE;
SELECT * FROM im ORDER BY id;
(INFO FOR TABLE im).migrations.guaranteed;
-- @check
-- Redefining the computed field over the misfit value: its values change at once and nothing is stored.
DEFINE FIELD OVERWRITE strict ON im TYPE option<int> COMPUTED IF type::is_int(b) THEN b + 10 ELSE -1 END;
SELECT * FROM im ORDER BY id;
-- @drain
(INFO FOR TABLE im).migrations.guaranteed;
-- @check
