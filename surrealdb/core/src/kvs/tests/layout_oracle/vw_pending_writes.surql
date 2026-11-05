-- Property: a write to a row behind a migration step reads the migrated value it depends
-- on, not the stored text: an increment, a compound assignment, a bulk UPDATE, an
-- INSERT ... ON DUPLICATE KEY UPDATE, an UPSERT, and a filtered DELETE. The view over
-- the table moves each row between its groups by the migrated values, so its sums and
-- counts agree with a scan after every write, before and after the job. The second part
-- repeats the upsert spellings on a row whose migrated field is read by an UPSERT, and the
-- third part runs the same spellings on a table that never migrates, as a control.
DEFINE TABLE ev SCHEMAFULL;
DEFINE FIELD amt ON ev TYPE string;
DEFINE FIELD kind ON ev TYPE string;
CREATE ev:1 SET amt = '10', kind = 'x' RETURN NONE;
CREATE ev:2 SET amt = '3', kind = 'x' RETURN NONE;
CREATE ev:3 SET amt = '20', kind = 'y' RETURN NONE;
CREATE ev:4 SET amt = 'none', kind = 'y' RETURN NONE;
DEFINE TABLE up SCHEMAFULL;
DEFINE FIELD amt ON up TYPE string;
CREATE up:2 SET amt = '3' RETURN NONE;
CREATE up:4 SET amt = 'none' RETURN NONE;
DEFINE TABLE pf SCHEMAFULL;
DEFINE FIELD amt ON pf TYPE int;
CREATE pf:4 SET amt = 0 RETURN NONE;
-- @upgrade
ALTER FIELD amt ON ev TYPE int MIGRATE EXISTING ROWS AS amt_int USING <int>$value FALLBACK 0;
ALTER FIELD amt ON up TYPE int MIGRATE EXISTING ROWS AS up_int USING <int>$value FALLBACK 0;
DEFINE TABLE by_kind AS SELECT kind, math::sum(amt) AS total, count() AS n FROM ev GROUP BY kind;
SELECT * FROM by_kind ORDER BY kind;
UPDATE ev:1 SET amt += 5 RETURN NONE;
SELECT * FROM by_kind ORDER BY kind;
UPDATE ev SET amt = amt * 2 WHERE kind = 'y' RETURN NONE;
SELECT VALUE id FROM ev WHERE amt > 10 ORDER BY id;
SELECT * FROM by_kind ORDER BY kind;
-- @check
INSERT INTO ev [{ id: ev:2, amt: '4', kind: 'x' }] ON DUPLICATE KEY UPDATE amt = amt + 100 RETURN NONE;
UPSERT ev:4 SET amt = amt + 1 RETURN NONE;
SELECT * FROM ev ORDER BY id;
SELECT * FROM by_kind ORDER BY kind;
DELETE ev WHERE amt > 30 RETURN NONE;
SELECT * FROM ev ORDER BY id;
SELECT * FROM by_kind ORDER BY kind;
-- @drain
SELECT * FROM by_kind ORDER BY kind;
SELECT * FROM ev ORDER BY id;
UPDATE ev SET kind = 'z' WHERE amt = 0 RETURN NONE;
SELECT * FROM by_kind ORDER BY kind;
-- @check
CREATE ev:5 SET amt = 7, kind = 'z' RETURN NONE;
UPDATE ev:2 SET kind = 'y' RETURN NONE;
SELECT * FROM by_kind ORDER BY kind;
SELECT VALUE id FROM ev WHERE kind = 'y' ORDER BY id;
-- @check
UPSERT up:2 SET amt = amt + 1 RETURN NONE;
UPSERT up:2 SET amt += 1 RETURN NONE;
SELECT * FROM up ORDER BY id;
UPSERT up:4 SET amt += 1 RETURN NONE;
UPSERT up:9 SET amt = 5 RETURN NONE;
SELECT * FROM up ORDER BY id;
-- @check
UPSERT pf:4 SET amt += 1 RETURN NONE;
UPSERT pf:4 SET amt = (amt ?? 0) + 1 RETURN NONE;
UPSERT pf:4 SET amt = amt + 1 RETURN NONE;
INSERT INTO pf [{ id: pf:4, amt: 9 }] ON DUPLICATE KEY UPDATE amt += 100 RETURN NONE;
INSERT INTO pf [{ id: pf:4, amt: 9 }] ON DUPLICATE KEY UPDATE amt = amt + 100 RETURN NONE;
SELECT * FROM pf ORDER BY id;
-- @check
