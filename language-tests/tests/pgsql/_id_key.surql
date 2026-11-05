/**
[test]
run = false
reason = "Setup only: tables whose record keys are integers, strings, uuids, arrays and objects, tables holding plain integer references to them, a table whose computed field raises on the row with key 2, and tables whose foreign keys are record links, plain keys or a mixture, for the id_key_* tests of both SQL dialects."
*/

DEFINE TABLE person SCHEMALESS;
CREATE person:1 SET name = 'Tobie', age = 42 RETURN NONE;
CREATE person:2 SET name = 'Jaime', age = 41 RETURN NONE;
CREATE person:3 SET name = 'Zed', age = 7 RETURN NONE;
CREATE person:10 SET name = 'Ten', age = 10 RETURN NONE;

DEFINE TABLE tag SCHEMALESS;
CREATE tag:rust SET label = 'Rust' RETURN NONE;
CREATE tag:db SET label = 'Database' RETURN NONE;
CREATE tag:`1` SET label = 'The string one' RETURN NONE;
CREATE tag:`person:1` SET label = 'A key that spells a record id' RETURN NONE;

DEFINE TABLE doc SCHEMALESS;
CREATE doc:u'018f6a52-0000-7000-8000-000000000001' SET title = 'First' RETURN NONE;
CREATE doc:u'018f6a52-0000-7000-8000-000000000002' SET title = 'Second' RETURN NONE;

DEFINE TABLE cell SCHEMALESS;
CREATE cell:[1, 2] SET v = 'array' RETURN NONE;
CREATE cell:{ x: 1 } SET v = 'object' RETURN NONE;

DEFINE TABLE profile SCHEMALESS;
CREATE profile:1 SET bio = 'Founder' RETURN NONE;
CREATE profile:3 SET bio = 'Tester' RETURN NONE;
CREATE profile:99 SET bio = 'Nobody' RETURN NONE;

DEFINE TABLE post SCHEMALESS;
DEFINE INDEX post_author ON post FIELDS author;
CREATE post:1 SET author = 1, title = 'Hello' RETURN NONE;
CREATE post:2 SET author = 1, title = 'World' RETURN NONE;
CREATE post:3 SET author = 2, title = 'Again' RETURN NONE;
CREATE post:4 SET author = 7, title = 'Orphan' RETURN NONE;
CREATE post:5 SET title = 'Anonymous' RETURN NONE;

DEFINE TABLE guarded SCHEMALESS;
DEFINE FIELD trap ON guarded COMPUTED IF record::id(id) = 2 { THROW 'the row with key 2 was read' } ELSE { 0 };
CREATE guarded:1 SET n = 1 RETURN NONE;
CREATE guarded:2 SET n = 2 RETURN NONE;
CREATE guarded:3 SET n = 3 RETURN NONE;

DEFINE TABLE ticket SCHEMALESS;
CREATE ticket:1 SET author = person:1, title = 'Link one' RETURN NONE;
CREATE ticket:2 SET author = person:1, title = 'Link two' RETURN NONE;
CREATE ticket:3 SET author = person:2, title = 'Link three' RETURN NONE;
CREATE ticket:4 SET author = person:99, title = 'Dangling link' RETURN NONE;
CREATE ticket:5 SET title = 'Unassigned' RETURN NONE;

DEFINE TABLE note SCHEMALESS;
CREATE note:1 SET ref = 1, text = 'plain key' RETURN NONE;
CREATE note:2 SET ref = person:2, text = 'link' RETURN NONE;
CREATE note:3 SET ref = 3, text = 'plain key again' RETURN NONE;
CREATE note:4 SET ref = NULL, text = 'null' RETURN NONE;
CREATE note:5 SET ref = 1.5, text = 'float' RETURN NONE;
CREATE note:6 SET ref = tag:rust, text = 'link to another table' RETURN NONE;

DEFINE TABLE node SCHEMALESS;
CREATE node:1 SET up = node:2 RETURN NONE;
CREATE node:2 SET up = 3 RETURN NONE;
CREATE node:3 SET up = node:3 RETURN NONE;
CREATE node:4 SET up = 4 RETURN NONE;
CREATE node:5 SET up = NULL RETURN NONE;
CREATE node:6 RETURN NONE;
