/**
[test]
issue = 4222
reason = "Public issue surrealdb/surrealdb#4222: a write to a table whose UNIQUE index was corrupt panicked on the insert path. A write refused by a unique entry reports the record that holds the value; only a refused compare is taken for a held value, any other storage error fails the write, and an entry that refuses a write yet reads as absent fails it as a corrupt index rather than panicking. A corrupt index cannot be produced from SurrealQL, so this pins the duplicate path on both the create and the update side."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
error = "Database index `u` already contains 1, with record `t:1`"

[[test.results]]
value = "[]"

[[test.results]]
error = "Database index `u` already contains 1, with record `t:1`"

[[test.results]]
value = "[t:1]"
*/
DEFINE TABLE t SCHEMALESS;
DEFINE INDEX u ON t FIELDS x UNIQUE;
CREATE t:1 SET x = 1 RETURN NONE;
CREATE t:2 SET x = 1 RETURN NONE;
CREATE t:3 SET x = 3 RETURN NONE;
UPDATE t:3 SET x = 1 RETURN NONE;
SELECT VALUE id FROM t WHERE x = 1;
