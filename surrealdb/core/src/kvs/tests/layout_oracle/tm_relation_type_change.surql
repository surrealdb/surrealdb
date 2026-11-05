-- A plain table becomes a relation, and back, after its rows have been migrated: a relation whose
-- rows fit its ends is accepted and its rows keep their migrated values; a plain record the ends
-- would not take refuses the change and leaves the table and its rows as they were; and a change
-- back to NORMAL keeps every row, edge or not.
DEFINE TABLE person SCHEMAFULL;
DEFINE FIELD name ON person TYPE string;
DEFINE TABLE ln SCHEMAFULL;
DEFINE FIELD w ON ln TYPE string;
DEFINE FIELD in ON ln TYPE record;
DEFINE FIELD out ON ln TYPE record;
CREATE person:1 SET name = 'a' RETURN NONE;
CREATE person:2 SET name = 'b' RETURN NONE;
CREATE person:3 SET name = 'c' RETURN NONE;
CREATE ln:1 SET w = '5', in = person:1, out = person:2 RETURN NONE;
CREATE ln:2 SET w = 'x', in = person:2, out = person:3 RETURN NONE;
-- @upgrade
ALTER FIELD w ON ln TYPE int MIGRATE EXISTING ROWS AS w_int USING <int>$value FALLBACK -1;
SELECT * FROM ln ORDER BY id;
-- @drain
SELECT * FROM ln ORDER BY id;
(INFO FOR TABLE ln).migrations;
-- Every row fits the person ends: the relation is accepted.
DEFINE TABLE OVERWRITE ln TYPE RELATION IN person OUT person ENFORCED SCHEMAFULL;
SELECT * FROM ln ORDER BY id;
SELECT VALUE w FROM ln ORDER BY id;
SELECT VALUE ->ln->person.id FROM person ORDER BY id;
(INFO FOR TABLE ln).migrations;
-- @check
-- New edges are written under the relation's rules.
RELATE person:3->ln:4->person:1 SET w = 8 RETURN NONE;
CREATE ln:3 SET w = 7, in = person:1, out = person:2 RETURN NONE;
SELECT VALUE id FROM ln ORDER BY id;
-- @drain
SELECT VALUE id FROM ln ORDER BY id;
-- Back to NORMAL: every row is kept, and the ends are declared wide again, so a plain record with a
-- non-person end can be written.
DEFINE TABLE OVERWRITE ln TYPE NORMAL SCHEMAFULL;
DEFINE FIELD OVERWRITE in ON ln TYPE record;
DEFINE FIELD OVERWRITE out ON ln TYPE record;
SELECT VALUE id FROM ln ORDER BY id;
CREATE ln:5 SET w = 9, in = hub:1, out = person:1 RETURN NONE;
SELECT VALUE id FROM ln ORDER BY id;
-- The relation is refused while a row's end is not a person: the table stays NORMAL.
DEFINE TABLE OVERWRITE ln TYPE RELATION IN person OUT person ENFORCED SCHEMAFULL;
SELECT VALUE id FROM ln ORDER BY id;
(INFO FOR TABLE ln).migrations;
-- With that row removed, the relation is accepted again and reads the rows it holds.
DELETE ln:5 RETURN NONE;
SELECT VALUE id FROM ln ORDER BY id;
DEFINE TABLE OVERWRITE ln TYPE RELATION IN person OUT person ENFORCED SCHEMAFULL;
SELECT VALUE id FROM ln ORDER BY id;
SELECT VALUE w FROM ln ORDER BY id;
-- @check
-- A migration of the relation's data field after the change: its rows are verified and migrated.
ALTER FIELD w ON ln TYPE int MIGRATE EXISTING ROWS AS w_int2 USING <int>$value * 10 FALLBACK -2;
SELECT VALUE w FROM ln ORDER BY id;
-- @drain
SELECT * FROM ln ORDER BY id;
SELECT VALUE ->ln->person.id FROM person ORDER BY id;
(INFO FOR TABLE ln).migrations;
-- @check
