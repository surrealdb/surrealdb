-- A record link to a row of a table with a pending step: FETCH, a path through the link, a filter on the
-- path, an index on the link, a link to an absent row, and a link written inside the caller's own
-- transaction all answer the target's migrated value.
DEFINE TABLE person SCHEMAFULL;
DEFINE FIELD name ON person TYPE string;
DEFINE FIELD age ON person TYPE string;
DEFINE TABLE post SCHEMAFULL;
DEFINE FIELD title ON post TYPE string;
DEFINE FIELD author ON post TYPE option<record<person>>;
DEFINE FIELD co ON post TYPE option<array<record<person>>>;
CREATE person:1 SET name = 'Ann', age = '41' RETURN NONE;
CREATE person:2 SET name = 'Bob', age = '37' RETURN NONE;
CREATE person:3 SET name = 'Cy', age = 'old' RETURN NONE;
CREATE post:1 SET title = 'p1', author = person:1 RETURN NONE;
CREATE post:2 SET title = 'p2', author = person:2, co = [person:3, person:1] RETURN NONE;
CREATE post:3 SET title = 'p3', author = person:9 RETURN NONE;
CREATE post:4 SET title = 'p4' RETURN NONE;
-- @upgrade
CREATE post:5 SET title = 'p5', author = person:2 RETURN NONE;
ALTER FIELD age ON person TYPE int MIGRATE EXISTING ROWS AS age_int USING (<int>$value) + 1 FALLBACK 0;
SELECT VALUE author.age FROM post ORDER BY id;
SELECT VALUE co.age FROM post:2;
SELECT * FROM post ORDER BY id FETCH author;
SELECT VALUE id FROM post WHERE author.age > 40 ORDER BY id;
SELECT VALUE id FROM post WHERE author.age = 38 ORDER BY id;
(post:1).author.age;
SELECT VALUE author.age FROM ONLY post:2;
SELECT title, co FROM post:2 FETCH co;
-- @check
DEFINE INDEX by_author ON post FIELDS author;
SELECT VALUE id FROM post WHERE author = person:2 ORDER BY id;
SELECT VALUE id FROM post WHERE author = person:9 ORDER BY id;
-- @drain
SELECT VALUE author.age FROM post ORDER BY id;
UPDATE person:1 SET age = 50 RETURN AFTER;
SELECT VALUE author.age FROM post ORDER BY id;
DELETE person:2 RETURN BEFORE;
SELECT VALUE author.age FROM post ORDER BY id;
SELECT VALUE author FROM post ORDER BY id FETCH author;
-- @client
CREATE person:4 SET name = 'Dee', age = 20 RETURN NONE;
CREATE post:6 SET title = 'p6', author = person:4 RETURN NONE;
SELECT VALUE author.age FROM post:6;
-- @end
SELECT VALUE author.age FROM post:6;
-- @check
