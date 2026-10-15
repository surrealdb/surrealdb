/**
[test]
wip = true
reason = "Not built: a TYPE change of `id` reads no keys, so a key that does not conform is not refused. The TYPE of `id` of an enforced table with rows is checked against the table's keys in the statement's own transaction: a change that a key does not satisfy is refused, naming the key, and leaves the definition as it was; a change every key satisfies is accepted."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
match = "string::contains($error, 'abc') && string::contains($error, '`id`')"
error = true

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
match = "(string::contains($error, 'tb:1') || string::contains($error, 'tb:2')) && string::contains($error, '`id`')"
error = true

[[test.results]]
match = "string::contains($error, 'abc') && string::contains($error, '`id`')"
error = true

[[test.results]]
value = "'DEFINE FIELD id ON `tb` TYPE int | string PERMISSIONS FULL'"

[[test.results]]
value = "[{ a: 'w', id: tb:abc }]"

[[test.results]]
value = "[{ a: 'v', id: tb:1 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "'DEFINE FIELD id ON `tb` TYPE int PERMISSIONS FULL'"

[[test.results]]
value = "[{ a: 'u', id: tb:1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The query was not executed due to a failed transaction"

[[test.results]]
error = "The query was not executed due to a failed transaction"

[[test.results]]
match = "string::contains($error, 'abc') && string::contains($error, '`id`')"
error = true

[[test.results]]
error = "Cannot COMMIT: the transaction was aborted due to a prior error"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "'DEFINE FIELD id ON em TYPE string PERMISSIONS FULL'"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ database: 'lvl3', namespace: 'test' }"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "'DEFINE FIELD id ON `tb` TYPE int PERMISSIONS FULL'"
*/
DEFINE TABLE tb SCHEMAFULL;
DEFINE FIELD a ON tb TYPE string;
CREATE tb:1 SET a = 'x' RETURN NONE;
CREATE tb:2 SET a = 'y' RETURN NONE;
CREATE tb:abc SET a = 'z' RETURN NONE;
DEFINE FIELD id ON tb TYPE int;
(INFO FOR TABLE tb).fields.id;
DEFINE FIELD id ON tb TYPE int | string;
DEFINE FIELD OVERWRITE id ON tb TYPE string;
ALTER FIELD id ON tb TYPE int;
(INFO FOR TABLE tb).fields.id;
UPDATE tb:abc SET a = 'w' RETURN AFTER;
UPDATE tb:1 SET a = 'v' RETURN AFTER;
DELETE tb:abc;
ALTER FIELD id ON tb TYPE int;
(INFO FOR TABLE tb).fields.id;
UPDATE tb:1 SET a = 'u' RETURN AFTER;
-- A key written earlier in the statement's own transaction is read too.
DEFINE TABLE tx SCHEMAFULL;
DEFINE FIELD a ON tx TYPE int;
BEGIN;
CREATE tx:1 SET a = 1;
CREATE tx:abc SET a = 2;
DEFINE FIELD id ON tx TYPE int;
COMMIT;
SELECT * FROM tx;
(INFO FOR TABLE tx).fields.id;
-- The same change on a table with no rows is accepted.
DEFINE TABLE em SCHEMAFULL;
DEFINE FIELD id ON em TYPE int;
DEFINE FIELD OVERWRITE id ON em TYPE string;
(INFO FOR TABLE em).fields.id;
-- Compatibility level 3 does not enforce: the same changes are accepted as they always were.
DEFINE DATABASE lvl3 COMPATIBILITY 3;
USE DB lvl3;
DEFINE TABLE tb SCHEMAFULL;
DEFINE FIELD a ON tb TYPE string;
CREATE tb:1 SET a = 'x' RETURN NONE;
CREATE tb:abc SET a = 'y' RETURN NONE;
DEFINE FIELD id ON tb TYPE int;
(INFO FOR TABLE tb).fields.id;
