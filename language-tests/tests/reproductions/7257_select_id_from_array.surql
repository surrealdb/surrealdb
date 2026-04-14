/**
[env]
namespace = true
database = true

[test]
reason = "SELECT id FROM [$rid] errored with 'Specify a database to use' in v3.0.5"
issue = 7257

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: a:1 }]"

[[test.results]]
value = "[{ id: a:1 }]"

[[test.results]]
value = "[1, 2, 3]"
*/

DEFINE TABLE a SCHEMAFULL;

LET $a = (CREATE ONLY a:1).id;

SELECT id FROM [$a];

SELECT id FROM $a;

SELECT * FROM [1, 2, 3];
