-- In-place upgrade across namespaces and databases. Each database keeps the level it was
-- created at, the legacy rows of every database read unchanged after the upgrade, and
-- ALTER DATABASE COMPATIBILITY 4 on one database queues a verification and rewrites rows only
-- there, leaving the legacy rows of the other databases alone. A database created after the
-- upgrade takes the default level, which ALTER SYSTEM COMPATIBILITY sets.
DEFINE NAMESPACE other;
USE NS other;
DEFINE DATABASE alpha;
DEFINE DATABASE beta COMPATIBILITY 3;
USE DB alpha;
DEFINE TABLE item SCHEMAFULL;
DEFINE FIELD n ON item TYPE int;
DEFINE INDEX by_n ON item FIELDS n;
CREATE item:1 SET n = 1;
CREATE item:2 SET n = 2;
CREATE item:3 SET n = -3;
USE DB beta;
DEFINE TABLE item SCHEMAFULL;
DEFINE FIELD n ON item TYPE string;
CREATE item:1 SET n = 'beta-1';
CREATE item:2 SET n = 'beta-2';
ALTER FIELD n ON item TYPE int;
USE NS test DB test;
DEFINE TABLE item SCHEMAFULL;
DEFINE FIELD n ON item TYPE string;
CREATE item:1 SET n = 'test-1';
DEFINE DATABASE gamma;
-- @upgrade
SELECT * FROM item ORDER BY id;
(INFO FOR DB).compatibility;
USE NS other DB alpha;
SELECT * FROM item ORDER BY id;
SELECT VALUE id FROM item WHERE n = 2 ORDER BY id;
(INFO FOR DB).compatibility;
USE DB beta;
SELECT * FROM item ORDER BY id;
(INFO FOR DB).compatibility;
-- @check
USE DB alpha;
ALTER DATABASE alpha COMPATIBILITY 4;
(INFO FOR DB).compatibility;
SELECT * FROM item ORDER BY id;
(INFO FOR TABLE item).migrations;
REMOVE INDEX by_n ON item;
ALTER FIELD n ON item TYPE string MIGRATE EXISTING ROWS AS n_str USING <string>$value FALLBACK 'none';
-- @drain
DEFINE INDEX by_n ON item FIELDS n;
SELECT * FROM item ORDER BY id;
SELECT VALUE id FROM item WHERE n = '2' ORDER BY id;
(INFO FOR TABLE item).migrations;
-- @check
USE DB beta;
SELECT * FROM item ORDER BY id;
CREATE item:3 SET n = 'x';
CREATE item:3 SET n = 3;
SELECT * FROM item ORDER BY id;
(INFO FOR TABLE item).migrations;
-- @check
USE NS test DB test;
DEFINE DATABASE epsilon;
(INFO FOR DB).compatibility;
INFO FOR NS;
ALTER SYSTEM COMPATIBILITY 4;
DEFINE DATABASE zeta;
(INFO FOR DB).compatibility;
INFO FOR NS;
USE NS other DB beta;
DEFINE DATABASE delta;
INFO FOR NS;
-- @check
SELECT * FROM item ORDER BY id;
-- @drain
-- @check
