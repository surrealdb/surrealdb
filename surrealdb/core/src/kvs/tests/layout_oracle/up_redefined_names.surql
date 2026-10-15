-- Names removed while legacy rows exist and defined again after the upgrade must not show the
-- rows they held: a table removed and defined again in the same database, a database removed
-- and defined again at another compatibility level, and a namespace removed and defined again.
-- The redefined name starts empty, its indexes answer only its own rows, and a neighbouring
-- table or database keeps its legacy rows.
DEFINE NAMESPACE scratch;
USE NS scratch;
DEFINE DATABASE d1 COMPATIBILITY 3;
USE DB d1;
DEFINE TABLE ghost SCHEMAFULL;
DEFINE FIELD v ON ghost TYPE int;
DEFINE INDEX g_v ON ghost FIELDS v;
CREATE ghost:1 SET v = 1;
CREATE ghost:2 SET v = 2;
DEFINE TABLE other SCHEMALESS;
CREATE other:1 SET w = 'keep';
USE NS scratch DB d1;
USE NS test DB test;
DEFINE TABLE item SCHEMAFULL;
DEFINE FIELD n ON item TYPE int;
CREATE item:1 SET n = 1;
-- @upgrade
USE NS scratch DB d1;
REMOVE TABLE ghost;
SELECT * FROM ghost;
DEFINE TABLE ghost SCHEMAFULL;
DEFINE FIELD v ON ghost TYPE string;
SELECT * FROM ghost;
SELECT VALUE id FROM ghost WHERE v = '1' ORDER BY id;
SELECT * FROM other ORDER BY id;
-- @check
USE NS scratch DB d1;
CREATE ghost:3 SET v = 'three';
SELECT * FROM ghost ORDER BY id;
-- @drain
SELECT * FROM ghost ORDER BY id;
USE NS scratch;
REMOVE DATABASE d1;
DEFINE DATABASE d1 COMPATIBILITY 4;
USE DB d1;
SELECT * FROM ghost;
DEFINE TABLE ghost SCHEMAFULL;
DEFINE FIELD v ON ghost TYPE int;
SELECT * FROM ghost;
INFO FOR DB;
-- @check
USE NS test DB test;
REMOVE NAMESPACE scratch;
DEFINE NAMESPACE scratch;
USE NS scratch;
DEFINE DATABASE d1;
USE DB d1;
SELECT * FROM ghost;
SELECT * FROM other;
DEFINE TABLE ghost SCHEMAFULL;
DEFINE FIELD v ON ghost TYPE int;
DEFINE INDEX g_v ON ghost FIELDS v;
SELECT VALUE id FROM ghost WHERE v = 2 ORDER BY id;
CREATE ghost:9 SET v = 2;
SELECT VALUE id FROM ghost WHERE v = 2 ORDER BY id;
-- @check
USE NS test DB test;
SELECT * FROM item ORDER BY id;
-- @drain
SELECT * FROM item ORDER BY id;
-- @check
