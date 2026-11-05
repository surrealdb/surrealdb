-- A normal table whose rows hold in and out fields is turned into a relation with no
-- step behind it. Its existing rows were never edges: no traversal may report them as
-- edges, and what the database exports must import again.
DEFINE TABLE person SCHEMAFULL;
DEFINE TABLE org SCHEMAFULL;
DEFINE TABLE plain SCHEMAFULL;
DEFINE FIELD in ON plain TYPE record<person>;
DEFINE FIELD out ON plain TYPE record<org>;
DEFINE FIELD since ON plain TYPE string;
CREATE person:1 RETURN NONE;
CREATE org:1 RETURN NONE;
CREATE plain:1 SET in = person:1, out = org:1, since = '4' RETURN NONE;
CREATE plain:2 SET in = person:1, out = org:1, since = '5' RETURN NONE;
-- @upgrade
ALTER TABLE plain TYPE RELATION;
SELECT * FROM plain ORDER BY id;
person:1->plain.since;
RELATE person:1->plain:3->org:1 SET since = '6' RETURN NONE;
SELECT * FROM plain ORDER BY id;
person:1->plain.since;
-- @check
