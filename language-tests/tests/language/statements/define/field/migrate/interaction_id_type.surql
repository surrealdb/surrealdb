/**
[test]
reason = "The TYPE of `id`, changed on a table whose keys all conform, is accepted on an enforced table with or without rows, and on a table that is not enforced; a migration of `id` is refused whatever the table."

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
value = "'DEFINE FIELD id ON e TYPE int | string PERMISSIONS FULL'"

[[test.results]]
value = "{ generation: 2, guaranteed: [], migrations: [] }"

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
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[tb:1, tb:2]"

[[test.results]]
value = "'DEFINE FIELD id ON `tb` TYPE int PERMISSIONS FULL'"

[[test.results]]
value = "{ generation: 2, guaranteed: [], migrations: [] }"

[[test.results]]
value = "[{ a: 'w', id: tb:1 }]"

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
value = "NONE"

[[test.results]]
value = "[{ a: 'w', id: sk:a }]"

[[test.results]]
error = "Cannot migrate field `id` of table `tb`: `id` is fixed by the record's key"

[[test.results]]
error = "Cannot migrate field `id` of table `sk`: `id` is fixed by the record's key"

[[test.results]]
error = "Cannot migrate field `id` of table `e`: `id` is fixed by the record's key"

[[test.results]]
value = "{ generation: 2, guaranteed: [], migrations: [] }"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[loose:1, loose:abc]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Cannot migrate field `id` of table `loose`: `id` is fixed by the record's key"

[[test.results]]
value = "[loose:1, loose:abc]"

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
value = "NONE"

[[test.results]]
value = "[tb:1, tb:abc]"

[[test.results]]
error = "Cannot migrate field `id` of table `tb`: `id` is fixed by the record's key"

[[test.results]]
value = "NONE"
*/
-- An enforced table with no rows: nothing to conform.
DEFINE TABLE e SCHEMAFULL;
DEFINE FIELD a ON e TYPE string;
DEFINE FIELD id ON e TYPE int;
DEFINE FIELD OVERWRITE id ON e TYPE string;
ALTER FIELD id ON e TYPE int | string;
(INFO FOR TABLE e).fields.id;
(INFO FOR TABLE e).migrations;
-- An enforced table whose keys all conform.
DEFINE TABLE tb SCHEMAFULL;
DEFINE FIELD a ON tb TYPE string;
CREATE tb:1 SET a = 'x' RETURN NONE;
CREATE tb:2 SET a = 'y' RETURN NONE;
DEFINE FIELD id ON tb TYPE int;
DEFINE FIELD OVERWRITE id ON tb TYPE int | string;
ALTER FIELD id ON tb TYPE number;
ALTER FIELD id ON tb TYPE int;
SELECT VALUE id FROM tb ORDER BY id;
(INFO FOR TABLE tb).fields.id;
(INFO FOR TABLE tb).migrations;
-- The rows keep their keys and stay writable.
UPDATE tb:1 SET a = 'w' RETURN AFTER;
-- A table made string-keyed.
DEFINE TABLE sk SCHEMAFULL;
DEFINE FIELD a ON sk TYPE string;
CREATE sk:a SET a = 'x' RETURN NONE;
CREATE sk:b SET a = 'y' RETURN NONE;
DEFINE FIELD id ON sk TYPE string;
ALTER FIELD id ON sk TYPE string | int;
UPDATE sk:a SET a = 'w' RETURN AFTER;
-- A migration of `id` is refused, whatever the table.
ALTER FIELD id ON tb TYPE string MIGRATE EXISTING ROWS AS id_str USING <string>$value FALLBACK '';
DEFINE FIELD OVERWRITE id ON sk TYPE string MIGRATE EXISTING ROWS AS id_str USING $value FALLBACK '';
ALTER FIELD id ON e MIGRATE EXISTING ROWS AS id_str USING $value;
(INFO FOR TABLE tb).migrations;
-- A table that is not enforced accepts the change, as it always has.
DEFINE TABLE loose SCHEMALESS;
CREATE loose:1 SET a = 'x' RETURN NONE;
CREATE loose:abc SET a = 'y' RETURN NONE;
DEFINE FIELD id ON loose TYPE int;
SELECT VALUE id FROM loose ORDER BY id;
DEFINE FIELD OVERWRITE id ON loose TYPE string;
ALTER FIELD id ON loose TYPE uuid;
ALTER FIELD id ON loose MIGRATE EXISTING ROWS AS id_str USING $value;
SELECT VALUE id FROM loose ORDER BY id;
-- The same at compatibility level 3.
DEFINE DATABASE lvl3 COMPATIBILITY 3;
USE DB lvl3;
DEFINE TABLE tb SCHEMAFULL;
DEFINE FIELD a ON tb TYPE string;
CREATE tb:1 SET a = 'x' RETURN NONE;
CREATE tb:abc SET a = 'y' RETURN NONE;
DEFINE FIELD id ON tb TYPE int;
DEFINE FIELD OVERWRITE id ON tb TYPE string;
SELECT VALUE id FROM tb ORDER BY id;
ALTER FIELD id ON tb TYPE string MIGRATE EXISTING ROWS AS id_str USING $value FALLBACK '';
(INFO FOR TABLE tb).migrations;
