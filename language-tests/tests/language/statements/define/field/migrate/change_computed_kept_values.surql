/**
[test]
reason = "A COMPUTED field reads nothing stored, but its path can still hold the values rows were written with before it was computed: a change at level 3 keeps them, and raising the database to level 4 rewrites nothing. At level 4 a COMPUTED field made stored reads as absent in every row, a migration from one reads `$value` as absent, and removing one clears its path, so no kept value resurfaces."

[[test.results]]
value = "NONE"

[[test.results]]
match = "$result.database == 'lvl3'"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ f: 'computed', id: t:1 }]"

[[test.results]]
value = "[{ f: '1', id: m:1 }]"

[[test.results]]
value = "[{ f: 'computed', id: r:1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: t:1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ f: -1, id: m:1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: r:1 }]"

[[test.results]]
value = "['remove f after 3']"
*/
-- @upgrade
DEFINE DATABASE lvl3 COMPATIBILITY 3;
USE DB lvl3;
DEFINE TABLE t SCHEMAFULL;
DEFINE FIELD f ON t TYPE string;
CREATE t:1 SET f = 'kept' RETURN NONE;
DEFINE FIELD OVERWRITE f ON t COMPUTED 'computed';
DEFINE TABLE m SCHEMAFULL;
DEFINE FIELD f ON m TYPE string;
CREATE m:1 SET f = '7' RETURN NONE;
DEFINE FIELD OVERWRITE f ON m COMPUTED '1';
DEFINE TABLE r SCHEMAFULL;
DEFINE FIELD f ON r TYPE string;
CREATE r:1 SET f = 'kept' RETURN NONE;
DEFINE FIELD OVERWRITE f ON r COMPUTED 'computed';
SELECT * FROM t;
SELECT * FROM m;
SELECT * FROM r;
ALTER DATABASE COMPATIBILITY 4;
-- Made stored: every row reads the field as absent.
DEFINE FIELD OVERWRITE f ON t TYPE option<string>;
SELECT * FROM t;
-- Migrated: `$value` reads the field as absent, so the FALLBACK is taken.
DEFINE FIELD OVERWRITE f ON m TYPE int MIGRATE EXISTING ROWS AS m_int USING <int>$value FALLBACK -1;
SELECT * FROM m;
-- Removed: nothing kept beneath it resurfaces.
REMOVE FIELD f ON r;
SELECT * FROM r;
(INFO FOR TABLE r).migrations.migrations.*.name;
