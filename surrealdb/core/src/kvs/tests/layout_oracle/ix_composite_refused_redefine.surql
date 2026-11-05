-- A composite index whose trailing column is being migrated refuses the migration while it is
-- defined. Once removed, the migration goes through and the composite index, defined again, answers
-- equality on the leading column with ranges on the migrated trailing column. A second composite,
-- led by the migrated column, must answer ranges on that column as integers.
DEFINE TABLE cp SCHEMAFULL;
DEFINE FIELD g ON cp TYPE string;
DEFINE FIELD a ON cp TYPE string;
CREATE cp:1 SET g = 'x', a = '9' RETURN NONE;
CREATE cp:2 SET g = 'x', a = '10' RETURN NONE;
CREATE cp:3 SET g = 'x', a = 'junk' RETURN NONE;
CREATE cp:4 SET g = 'y', a = '12' RETURN NONE;
-- @upgrade
DEFINE INDEX cg ON cp FIELDS g, a;
ALTER FIELD a ON cp TYPE int MIGRATE EXISTING ROWS AS a_int USING <int>$value FALLBACK 0;
(INFO FOR TABLE cp).migrations;
SELECT VALUE a FROM cp ORDER BY id;
REMOVE INDEX cg ON cp;
ALTER FIELD a ON cp TYPE int MIGRATE EXISTING ROWS AS a_int USING <int>$value FALLBACK 0;
DEFINE INDEX cg ON cp FIELDS g, a;
DEFINE INDEX ag ON cp FIELDS a, g;
SELECT VALUE id FROM cp WHERE g = 'x' AND a > 9 ORDER BY id;
SELECT VALUE id FROM cp WHERE g = 'x' AND a > 9 ORDER BY id EXPLAIN;
SELECT VALUE id FROM cp WHERE g = 'x' AND a = 0 ORDER BY id;
SELECT VALUE id FROM cp WHERE g = 'y' AND a >= 12 ORDER BY id;
SELECT VALUE id FROM cp WHERE a > 9 AND g = 'x' ORDER BY id;
SELECT VALUE id FROM cp WHERE a >= 0 AND a < 12 ORDER BY id;
-- @check
UPDATE cp:3 SET g = 'y' RETURN NONE;
SELECT VALUE id FROM cp WHERE g = 'y' AND a = 0 ORDER BY id;
CREATE cp:5 SET g = 'x', a = 11 RETURN NONE;
DELETE cp:2;
SELECT VALUE id FROM cp WHERE g = 'x' AND a > 9 ORDER BY id;
SELECT VALUE id FROM cp WHERE a > 9 ORDER BY id;
-- @drain
SELECT VALUE id FROM cp WHERE g = 'x' AND a > 9 ORDER BY id;
SELECT VALUE a FROM cp ORDER BY id;
-- @check
