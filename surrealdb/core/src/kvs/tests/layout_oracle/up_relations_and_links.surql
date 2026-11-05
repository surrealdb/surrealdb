-- Graph edges and record links written before the upgrade: a typed relation table with
-- fields, a lightweight relation, a record link and an array of record links. After the
-- upgrade, traversals, fetches and scans of the edges must follow the legacy in and out
-- pointers, a migration over an edge's field must reach the legacy edge rows, and an edge
-- updated after the upgrade must keep its endpoints.
DEFINE TABLE person SCHEMAFULL;
DEFINE FIELD name ON person TYPE string;
DEFINE FIELD friends ON person TYPE option<array<record<person>>>;
DEFINE TABLE likes TYPE RELATION FROM person TO person SCHEMAFULL;
DEFINE FIELD weight ON likes TYPE int;
DEFINE TABLE follows TYPE RELATION IN person OUT person LIGHTWEIGHT;
CREATE person:tobie SET name = 'Tobie';
CREATE person:jaime SET name = 'Jaime', friends = [person:tobie];
CREATE person:ann SET name = 'Ann';
RELATE person:tobie->likes:l1->person:jaime SET weight = 3;
RELATE person:jaime->likes:l2->person:ann SET weight = 1;
RELATE person:ann->likes:l3->person:tobie SET weight = 2;
RELATE person:tobie->follows->person:ann;
RELATE person:jaime->follows->person:tobie;
RELATE person:ann->follows->person:jaime;
-- @upgrade
SELECT name, ->likes->person.name AS liked FROM person ORDER BY id;
SELECT VALUE <-follows<-person.name FROM person:ann;
SELECT VALUE ->follows->person FROM person:tobie;
SELECT * FROM likes ORDER BY id;
SELECT VALUE id FROM likes WHERE in = person:tobie ORDER BY id;
SELECT VALUE friends FROM person:jaime;
SELECT * FROM person:jaime FETCH friends;
SELECT in, out FROM follows ORDER BY in, out;
-- @check
UPDATE likes SET weight = weight + 10 WHERE out = person:ann;
SELECT * FROM likes ORDER BY id;
UPDATE person:ann SET friends = [person:tobie, person:jaime] RETURN AFTER;
SELECT name, ->likes->person.name AS liked FROM person ORDER BY id;
ALTER FIELD weight ON likes TYPE float MIGRATE EXISTING ROWS AS w_f USING <float>$value FALLBACK 0.0;
-- @drain
SELECT * FROM likes ORDER BY id;
SELECT VALUE ->likes->person.name FROM person:tobie;
-- @check
RELATE person:tobie->likes:l4->person:ann SET weight = 7.5;
UPDATE likes:l1 SET weight = 4.25 RETURN AFTER;
SELECT * FROM likes ORDER BY id;
SELECT VALUE in FROM follows WHERE out = person:jaime ORDER BY in;
DELETE person:jaime;
SELECT name, ->likes->person.name AS liked FROM person ORDER BY id;
SELECT in, out FROM follows ORDER BY in, out;
-- @check
