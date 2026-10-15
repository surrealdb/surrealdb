-- A LIGHTWEIGHT relation stores no records of its own; its edges are read from the
-- adjacency of its IN tables. Its endpoints are records of a table with a pending
-- migration. A traversal through the edge must dereference each endpoint as the step
-- leaves it, and a record created at a deleted endpoint id takes the current layout.
DEFINE TABLE person SCHEMAFULL;
DEFINE FIELD age ON person TYPE string;
DEFINE TABLE follows TYPE RELATION IN person OUT person LIGHTWEIGHT;
CREATE person:1 SET age = '30' RETURN NONE;
CREATE person:2 SET age = '41' RETURN NONE;
CREATE person:3 SET age = 'x' RETURN NONE;
RELATE person:1->follows->person:2 RETURN NONE;
RELATE person:2->follows->person:3 RETURN NONE;
RELATE person:3->follows->person:1 RETURN NONE;
-- @upgrade
ALTER FIELD age ON person TYPE int MIGRATE EXISTING ROWS AS age_int USING <int>$value FALLBACK -1;
person:1->follows->person.age;
SELECT VALUE ->follows->person.age FROM person ORDER BY id;
SELECT VALUE <-follows<-person.age FROM person ORDER BY id;
SELECT * FROM follows;
-- @check
DELETE person:2 RETURN BEFORE;
SELECT VALUE ->follows->person.age FROM person ORDER BY id;
person:1->follows->person.age;
-- @drain
SELECT VALUE ->follows->person.age FROM person ORDER BY id;
CREATE person:2 SET age = 77 RETURN NONE;
person:1->follows->person.age;
person:3->follows->person.age;
SELECT VALUE <-follows<-person.age FROM person ORDER BY id;
-- @check
RELATE person:1->follows->person:3 RETURN NONE;
SELECT VALUE ->follows->person.age FROM person:1;
UPDATE person:3 SET age = 88 RETURN AFTER;
SELECT VALUE ->follows->person.age FROM person ORDER BY id;
-- @check
