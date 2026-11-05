-- A text field on an edge, migrated to a record link into another table. The index
-- that reads the field is removed before the migration. Dereferencing the link (FETCH,
-- a path through it, a filter on it) and index lookups on it must read the migrated
-- link once the job has run, and must agree with a scan before it has. A text that
-- does not name an org falls back to NONE, and a text naming a person is not an org link.
DEFINE TABLE person SCHEMAFULL;
DEFINE FIELD name ON person TYPE string;
DEFINE TABLE org SCHEMAFULL;
DEFINE FIELD size ON org TYPE int;
DEFINE TABLE hosts TYPE RELATION IN person OUT org SCHEMAFULL;
DEFINE FIELD ref ON hosts TYPE option<string>;
CREATE person:1 SET name = 'ann' RETURN NONE;
CREATE person:2 SET name = 'bob' RETURN NONE;
CREATE org:1 SET size = 10 RETURN NONE;
CREATE org:2 SET size = 20 RETURN NONE;
CREATE org:3 SET size = 30 RETURN NONE;
RELATE person:1->hosts:1->org:1 SET ref = 'org:2' RETURN NONE;
RELATE person:1->hosts:2->org:3 SET ref = 'org:3' RETURN NONE;
RELATE person:2->hosts:3->org:1 SET ref = 'nope' RETURN NONE;
RELATE person:2->hosts:4->org:2 SET ref = 'person:1' RETURN NONE;
DEFINE INDEX by_ref ON hosts FIELDS ref;
-- @upgrade
RELATE person:2->hosts:5->org:3 SET ref = 'org:1' RETURN NONE;
-- The index reads ref, so the migration waits until the index is removed.
REMOVE INDEX by_ref ON hosts;
ALTER FIELD ref ON hosts TYPE option<record<org>> MIGRATE EXISTING ROWS AS ref_link USING <record<org>>$value FALLBACK NONE;
SELECT id, ref FROM hosts ORDER BY id;
SELECT VALUE id FROM hosts WHERE ref = org:2 ORDER BY id;
SELECT VALUE ref.size FROM hosts ORDER BY id;
person:1->hosts[WHERE ref.size > 15].id;
-- Defined again while the step still waits for the job.
DEFINE INDEX by_ref ON hosts FIELDS ref;
SELECT VALUE id FROM hosts WHERE ref = org:2 ORDER BY id;
-- @check
-- @drain
DEFINE INDEX OVERWRITE by_ref ON hosts FIELDS ref;
SELECT * FROM hosts ORDER BY id FETCH ref;
SELECT VALUE id FROM hosts WHERE ref = org:2 ORDER BY id;
SELECT VALUE ref.size FROM hosts ORDER BY id;
person:1->hosts[WHERE ref.size > 15]->org.size;
person:2->hosts.ref;
(INFO FOR TABLE hosts).migrations;
-- @check
-- Writes after the step: a link written by a statement, a link written by an update,
-- and a text refused by the type.
RELATE person:1->hosts:6->org:1 SET ref = org:9 RETURN NONE;
UPDATE hosts:3 SET ref = org:1 RETURN AFTER;
UPDATE hosts:4 SET ref = 'org:3' RETURN AFTER;
SELECT VALUE ref FROM hosts ORDER BY id;
SELECT VALUE id FROM hosts WHERE ref = org:1 ORDER BY id;
-- @check
