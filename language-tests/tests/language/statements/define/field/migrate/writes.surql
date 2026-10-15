/**
[test]
reason = "A write to an old row stores the migrated values, and two migrations in one transaction with writes between them each apply once, through the record cache."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ f: '3', id: t:1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ f: 3, g: 1, id: t:1 }]"

[[test.results]]
value = "[{ f: 3, g: 1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ f: 13, g: 1, id: t:1 }]"

[[test.results]]
value = "[{ f: 13, g: 2 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ f: 1, id: u:9 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ f: 2f, g: 1, id: u:9 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ f: 2f, is_float: true }]"
*/
DEFINE TABLE t SCHEMAFULL;
DEFINE FIELD f ON t TYPE string;
DEFINE FIELD g ON t TYPE option<int>;
CREATE t:1 SET f = '3';
ALTER FIELD f ON t TYPE int MIGRATE EXISTING ROWS AS f_int USING <int>$value FALLBACK 0;
UPDATE t:1 SET g = 1;
SELECT f, g FROM t:1;
ALTER FIELD f ON t TYPE int MIGRATE EXISTING ROWS AS f_inc USING $value + 10 FALLBACK 0;
UPDATE t:1 SET g = 2 RETURN BEFORE;
SELECT f, g FROM t:1;
DEFINE TABLE u SCHEMAFULL;
DEFINE FIELD f ON u TYPE int;
DEFINE FIELD g ON u TYPE option<int>;
BEGIN;
ALTER FIELD f ON u MIGRATE EXISTING ROWS AS plus_one USING $value + 1 FALLBACK 0;
CREATE u:9 SET f = 1;
ALTER FIELD f ON u TYPE float MIGRATE EXISTING ROWS AS times_two USING $value * 2 FALLBACK 0;
UPDATE u:9 SET g = 1;
COMMIT;
SELECT f, type::is_float(f) AS is_float FROM u:9;
