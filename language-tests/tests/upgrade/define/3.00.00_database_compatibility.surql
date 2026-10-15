/**
[env]
compatibility = 3
imports = ["upgrade/define/3.00.00_database_compatibility_import.surql"]

[test]
upgrade = true
reason = "A database a released binary defined stores no compatibility level and reads at level 3: its SCHEMAFULL table accepts a retype its rows do not satisfy, without MIGRATE. Raised to level 4 with ALTER DATABASE, which rewrites nothing, the table refuses one."
# The body's grammar is the current checkout's.
version = ">=3.4.0-a"

[[test.results]]
value = "3"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ name: 'a', qty: 1 }, { name: 'b', qty: 2 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "4"

[[test.results]]
value = "[{ name: 'a', qty: 1 }, { name: 'b', qty: 2 }]"

[[test.results]]
error = "Cannot change field `name` of table `item`: the table is SCHEMAFULL in a database at compatibility level 4 and has rows, and the new definition does not accept every value the old one did: add MIGRATE EXISTING ROWS"

[[test.results]]
value = "{ name: 'DEFINE FIELD name ON item TYPE string PERMISSIONS FULL', qty: 'DEFINE FIELD qty ON item TYPE string PERMISSIONS FULL' }"
*/
(INFO FOR DB).compatibility;
ALTER FIELD qty ON item TYPE string;
SELECT name, qty FROM item ORDER BY id;
ALTER DATABASE COMPATIBILITY 4;
(INFO FOR DB).compatibility;
SELECT name, qty FROM item ORDER BY id;
ALTER FIELD name ON item TYPE int;
(INFO FOR TABLE item).fields;
