-- A plain index defined over a migrated field before the job drains it holds the migrated
-- integers, so range, IN and ORDER BY ... LIMIT queries order '10' above '9' as integers in the
-- index and in the scan alike, and writes to rows behind the step move their index entries.
DEFINE TABLE rg SCHEMAFULL;
DEFINE FIELD a ON rg TYPE string;
DEFINE FIELD b ON rg TYPE int;
CREATE rg:1 SET a = '10', b = 1 RETURN NONE;
CREATE rg:2 SET a = '9', b = 2 RETURN NONE;
CREATE rg:3 SET a = '100', b = 3 RETURN NONE;
CREATE rg:4 SET a = 'junk', b = 4 RETURN NONE;
CREATE rg:5 SET a = '-3', b = 5 RETURN NONE;
CREATE rg:6 SET a = '2.5', b = 6 RETURN NONE;
-- @upgrade
ALTER FIELD a ON rg TYPE int MIGRATE EXISTING ROWS AS a_int USING <int>$value FALLBACK 0;
DEFINE INDEX ia ON rg FIELDS a;
DEFINE INDEX ib ON rg FIELDS b;
SELECT VALUE id FROM rg WHERE a > 9 ORDER BY id;
SELECT VALUE id FROM rg WHERE a >= 9 ORDER BY id;
SELECT VALUE id FROM rg WHERE a < 10 ORDER BY id;
SELECT VALUE id FROM rg WHERE a <= 10 ORDER BY id;
SELECT VALUE id FROM rg WHERE a > 0 AND a < 100 ORDER BY id;
SELECT VALUE id FROM rg WHERE a >= 0 AND a <= 10 ORDER BY id;
SELECT VALUE id FROM rg WHERE a IN [0, 9, 100] ORDER BY id;
SELECT VALUE id FROM rg WHERE a > 9 ORDER BY id EXPLAIN;
SELECT VALUE id FROM rg ORDER BY a LIMIT 2;
SELECT VALUE id FROM rg ORDER BY a DESC LIMIT 2;
SELECT VALUE id FROM rg WHERE b >= 3 AND b < 6 ORDER BY id;
-- @check
UPDATE rg:1 SET a = 50 RETURN NONE;
CREATE rg:7 SET a = 7, b = 7 RETURN NONE;
DELETE rg:2;
SELECT VALUE id FROM rg WHERE a >= 9 ORDER BY id;
SELECT VALUE id FROM rg WHERE a = 9 ORDER BY id;
SELECT VALUE id FROM rg ORDER BY a LIMIT 3;
-- @drain
SELECT VALUE a FROM rg ORDER BY id;
SELECT VALUE id FROM rg WHERE a > -5 ORDER BY id;
SELECT VALUE id FROM rg ORDER BY a LIMIT 2;
-- @check
DEFINE DATABASE lvl3 COMPATIBILITY 3;
USE DB lvl3;
DEFINE TABLE rl SCHEMAFULL;
DEFINE FIELD a ON rl TYPE string;
CREATE rl:1 SET a = '10' RETURN NONE;
CREATE rl:2 SET a = '9' RETURN NONE;
CREATE rl:3 SET a = 'junk' RETURN NONE;
ALTER FIELD a ON rl TYPE int MIGRATE EXISTING ROWS AS a_int USING <int>$value FALLBACK 0;
DEFINE INDEX ia ON rl FIELDS a;
SELECT VALUE id FROM rl WHERE a > 9 ORDER BY id;
SELECT VALUE id FROM rl WHERE a > 9 ORDER BY id EXPLAIN;
UPDATE rl:3 SET a = 12 RETURN NONE;
SELECT VALUE id FROM rl WHERE a > 9 ORDER BY id;
-- @drain
SELECT VALUE id FROM rl WHERE a > 9 ORDER BY id;
-- @check
