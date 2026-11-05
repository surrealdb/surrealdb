-- A self-relation: edges whose in and out are one record, and edges whose endpoints
-- are deleted while migrations on the endpoint table and on the edge table are pending.
-- Traversals in both directions must agree with edge scans, and a record created at a
-- deleted id after a step takes no replayed value.
DEFINE TABLE person SCHEMAFULL;
DEFINE FIELD name ON person TYPE string;
DEFINE FIELD age ON person TYPE string;
DEFINE TABLE follows TYPE RELATION IN person OUT person SCHEMAFULL;
DEFINE FIELD since ON follows TYPE string;
CREATE person:1 SET name = 'a', age = '30' RETURN NONE;
CREATE person:2 SET name = 'b', age = '41' RETURN NONE;
CREATE person:3 SET name = 'c', age = 'old' RETURN NONE;
RELATE person:1->follows:1->person:1 SET since = '2020' RETURN NONE;
RELATE person:1->follows:2->person:2 SET since = '2021' RETURN NONE;
RELATE person:2->follows:3->person:1 SET since = '2022' RETURN NONE;
RELATE person:3->follows:4->person:3 SET since = 'x' RETURN NONE;
-- @upgrade
ALTER FIELD age ON person TYPE int MIGRATE EXISTING ROWS AS age_int USING <int>$value FALLBACK -1;
ALTER FIELD since ON follows TYPE int MIGRATE EXISTING ROWS AS since_int USING <int>$value FALLBACK 0;
SELECT * FROM follows ORDER BY id;
person:1->follows->person.age;
SELECT VALUE <-follows<-person.name FROM person:1;
person:1->follows.since;
SELECT VALUE ->follows->person.id FROM person ORDER BY id;
SELECT VALUE id FROM follows WHERE in = out ORDER BY id;
-- @check
DELETE person:2 RETURN BEFORE;
SELECT * FROM follows ORDER BY id;
SELECT VALUE ->follows.since FROM person ORDER BY id;
person:1->follows->person.age;
-- A new record at the deleted id takes the current layout, not the replayed one.
CREATE person:2 SET name = 'b2', age = 50 RETURN NONE;
SELECT * FROM person ORDER BY id;
person:1->follows->person.age;
person:3->follows->person.age;
-- @drain
SELECT * FROM person ORDER BY id;
DELETE person:1 RETURN BEFORE;
SELECT * FROM follows ORDER BY id;
SELECT VALUE ->follows.since FROM person ORDER BY id;
person:2->follows->person.name;
RELATE person:3->follows:5->person:2 SET since = 2030 RETURN NONE;
SELECT VALUE id FROM follows WHERE since >= 2000 ORDER BY id;
person:3->follows.since;
-- @check
UPDATE follows:4 SET since = since + 1 RETURN AFTER;
DELETE follows:4 RETURN BEFORE;
SELECT * FROM follows ORDER BY id;
person:3->follows.since;
-- @check
