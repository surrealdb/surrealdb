-- A UNIQUE index over a migrated edge field. The index reads the field, so the field
-- may migrate only once the index is removed. Defining the index again while the step
-- waits must refuse the values that collapse onto one key under the step, and once
-- the job has run and the values are distinct, the index must accept them and refuse
-- an edge written afterwards that takes a key a surviving edge holds.
DEFINE TABLE person SCHEMAFULL;
DEFINE TABLE org SCHEMAFULL;
DEFINE TABLE works TYPE RELATION IN person OUT org SCHEMAFULL;
DEFINE FIELD since ON works TYPE string;
DEFINE INDEX uniq_since ON works FIELDS since UNIQUE;
CREATE person:1 RETURN NONE;
CREATE person:2 RETURN NONE;
CREATE org:1 RETURN NONE;
CREATE org:2 RETURN NONE;
CREATE org:3 RETURN NONE;
RELATE person:1->works:1->org:1 SET since = 'a' RETURN NONE;
RELATE person:1->works:2->org:2 SET since = 'b' RETURN NONE;
RELATE person:2->works:3->org:1 SET since = '5' RETURN NONE;
-- @upgrade
RELATE person:2->works:4->org:2 SET since = '6' RETURN NONE;
-- The index reads since, so the migration is refused while the index is defined.
ALTER FIELD since ON works TYPE int MIGRATE EXISTING ROWS AS since_int USING <int>$value FALLBACK 0;
REMOVE INDEX uniq_since ON works;
ALTER FIELD since ON works TYPE int MIGRATE EXISTING ROWS AS since_int USING <int>$value FALLBACK 0;
-- Defined again while the step waits: 'a' and 'b' both migrate to 0.
DEFINE INDEX uniq_since ON works FIELDS since UNIQUE;
SELECT * FROM works ORDER BY id;
SELECT VALUE id FROM works WHERE since = 0 ORDER BY id;
-- @check
-- @drain
SELECT * FROM works ORDER BY id;
SELECT VALUE id FROM works WHERE since = 0 ORDER BY id;
-- Once the values are distinct, the index is defined and holds them.
UPDATE works:2 SET since = 2 RETURN AFTER;
DEFINE INDEX uniq_since ON works FIELDS since UNIQUE;
(INFO FOR TABLE works).indexes;
SELECT VALUE id FROM works WHERE since = 2 ORDER BY id;
RELATE person:1->works:5->org:3 SET since = 9 RETURN NONE;
RELATE person:1->works:6->org:3 SET since = 9 RETURN NONE;
SELECT VALUE id FROM works WHERE since = 9 ORDER BY id;
SELECT * FROM works ORDER BY id;
-- @check
