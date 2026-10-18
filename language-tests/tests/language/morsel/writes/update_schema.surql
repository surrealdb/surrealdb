/**
[env]
namespace = true
database = true
# Every statement below writes hundreds of records, which the suite's forced configuration cuts into
# a batch for each record, and RocksDB does not finish that in the default timeout while the suite
# runs in parallel.
timeout = 30000

[test]
reason = "an UPDATE of a table with a schema applies its assertions, defaults, values and computed fields to each record as the serial run does, whatever width its compute half runs at"

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
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "['N0X', 'N1X', 'N2X', 'N3X']"

[[test.results]]
value = "[25, 36, 49]"

[[test.results]]
value = "['set', 'set', 'set']"

[[test.results]]
error = "Found '' for field `name`, with record `t:300`, but field must conform to: string::len($value) > 0"

[[test.results]]
error = "Couldn't coerce value for field `n` of `t:400`: Expected `int` but found `'not an int'`"

[[test.results]]
value = "[{ count: 600 }]"

*/

DEFINE TABLE t SCHEMAFULL;
DEFINE FIELD n ON t TYPE int;
DEFINE FIELD name ON t TYPE string ASSERT string::len($value) > 0;
DEFINE FIELD s ON t TYPE string DEFAULT 'none';
DEFINE FIELD up ON t VALUE string::uppercase(name);
DEFINE FIELD sq ON t COMPUTED n * n;
FOR $i IN 0..600 { CREATE type::record('t', $i) SET n = $i, name = 'n' + <string> $i };
UPDATE t SET name = name + 'x' RETURN NONE;
SELECT VALUE up FROM t:0..4;
SELECT VALUE sq FROM t:5..8;
UPDATE t SET s = 'set' WHERE n < 3 RETURN VALUE s;
UPDATE t SET name = '' WHERE n = 300;
UPDATE t SET n = 'not an int' WHERE n = 400;
SELECT count() FROM t WHERE string::ends_with(name, 'x') GROUP ALL;
