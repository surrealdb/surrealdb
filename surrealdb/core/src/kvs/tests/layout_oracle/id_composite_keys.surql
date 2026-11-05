-- Rows keyed by an array, an object, a tuple-typed array and a uuid keep their keys while a migrated field
-- of each is behind a step: a read by full id, by an id range, by ONLY and as a field of a record id
-- answers the migrated value, a write that leaves the field alone does not replay it again, and a row
-- created again under a deleted key is not replayed through the step at all.
DEFINE TABLE reading SCHEMAFULL;
DEFINE FIELD id ON reading TYPE [string, datetime];
DEFINE FIELD temp ON reading TYPE string;
DEFINE FIELD note ON reading TYPE option<string>;
CREATE reading:['London', d'2024-01-01T00:00:00Z'] SET temp = '11' RETURN NONE;
CREATE reading:['London', d'2024-01-02T00:00:00Z'] SET temp = '12' RETURN NONE;
CREATE reading:['Rome', d'2024-01-01T00:00:00Z'] SET temp = '9' RETURN NONE;
DEFINE TABLE shape SCHEMAFULL;
DEFINE FIELD w ON shape TYPE string;
DEFINE FIELD note ON shape TYPE option<string>;
CREATE shape:{ k: 'a', n: 1 } SET w = '3' RETURN NONE;
CREATE shape:{ k: 'a', n: 2 } SET w = '4' RETURN NONE;
CREATE shape:{ k: 'b', n: 1 } SET w = '5' RETURN NONE;
DEFINE TABLE dev SCHEMAFULL;
DEFINE FIELD v ON dev TYPE string;
CREATE dev:u'018f9a3c-0000-7000-8000-000000000001' SET v = '7' RETURN NONE;
CREATE dev:u'018f9a3c-0000-7000-8000-000000000002' SET v = '8' RETURN NONE;
-- @upgrade
CREATE reading:['Rome', d'2024-01-02T00:00:00Z'] SET temp = '10' RETURN NONE;
ALTER FIELD temp ON reading TYPE int MIGRATE EXISTING ROWS AS temp_x10 USING (<int>$value) * 10 FALLBACK -1;
ALTER FIELD w ON shape TYPE int MIGRATE EXISTING ROWS AS w_x10 USING (<int>$value) * 10 FALLBACK -1;
ALTER FIELD v ON dev TYPE int MIGRATE EXISTING ROWS AS v_x10 USING (<int>$value) * 10 FALLBACK -1;
SELECT * FROM reading ORDER BY id;
SELECT VALUE temp FROM ONLY reading:['London', d'2024-01-01T00:00:00Z'];
(reading:['Rome', d'2024-01-01T00:00:00Z']).temp;
SELECT VALUE temp FROM reading:['London']..=['London', d'2024-01-02T00:00:00Z'] ORDER BY id;
SELECT VALUE id FROM shape WHERE w > 35 ORDER BY id;
SELECT VALUE w FROM ONLY shape:{ k: 'a', n: 2 };
(shape:{ k: 'b', n: 1 }).w;
SELECT VALUE v FROM dev ORDER BY id;
(dev:u'018f9a3c-0000-7000-8000-000000000002').v;
-- @check
UPDATE reading:['London', d'2024-01-01T00:00:00Z'] SET note = 'a' RETURN AFTER;
SELECT VALUE temp FROM reading ORDER BY id;
UPDATE shape:{ k: 'a', n: 1 } SET note = 'b' RETURN AFTER;
SELECT VALUE w FROM shape ORDER BY id;
-- @drain
SELECT * FROM shape ORDER BY id;
DELETE shape:{ k: 'a', n: 2 } RETURN BEFORE;
CREATE shape:{ k: 'a', n: 2 } SET w = 6 RETURN AFTER;
SELECT VALUE w FROM shape ORDER BY id;
UPDATE dev:u'018f9a3c-0000-7000-8000-000000000001' SET v = 70 RETURN AFTER;
SELECT VALUE v FROM dev ORDER BY id;
-- @drain
SELECT VALUE temp FROM reading ORDER BY id;
SELECT VALUE w FROM shape ORDER BY id;
-- @check
