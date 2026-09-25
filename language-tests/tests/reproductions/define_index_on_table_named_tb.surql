/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "`TB` is a spelling of the `TABLE` keyword only where that keyword is mandatory. Where `ON [TABLE] <table>` makes it optional, a bare `tb` is the table name, so a table called `tb` is usable without backticks or an explicit `TABLE`"

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
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: 1, b: 2, id: tb:1 }]"

[[test.results]]
value = "[1]"

[[test.results]]
value = """{ events: { ev: "DEFINE EVENT ev ON `tb` WHEN $event = 'CREATE' THEN { RETURN NONE }" }, fields: { a: "DEFINE FIELD a ON `tb` TYPE none | int COMMENT 'first column' PERMISSIONS FULL", b: 'DEFINE FIELD b ON `tb` TYPE none | int PERMISSIONS FULL' }, indexes: { ix: 'DEFINE INDEX ix ON `tb` FIELDS a, b', ix2: 'DEFINE INDEX ix2 ON `tb` FIELDS a', ix3: 'DEFINE INDEX ix3 ON `tb` FIELDS b' }, lives: {  }, tables: {  } }"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"
*/
DEFINE TABLE tb SCHEMALESS;

-- The table position of `ON [TABLE] <table>`, bare in every statement that
-- offers the optional keyword.
DEFINE FIELD a ON tb TYPE option<int>;
DEFINE FIELD b ON tb TYPE option<int>;
DEFINE INDEX ix ON tb FIELDS a, b;
DEFINE EVENT ev ON tb WHEN $event = "CREATE" THEN { RETURN NONE };
ALTER FIELD a ON tb COMMENT "first column";
REBUILD INDEX ix ON tb;

-- The same statements keep parsing with the keyword written out and with the
-- name escaped, and all three spellings name one table.
DEFINE INDEX ix2 ON TABLE tb FIELDS a;
DEFINE INDEX ix3 ON `tb` FIELDS b;
CREATE tb:1 SET a = 1, b = 2;
SELECT VALUE a FROM tb WHERE b = 2;

-- `TB` stays an accepted spelling of the keyword where it is mandatory.
INFO FOR TB tb;

REMOVE INDEX ix3 ON tb;
REMOVE EVENT ev ON tb;
