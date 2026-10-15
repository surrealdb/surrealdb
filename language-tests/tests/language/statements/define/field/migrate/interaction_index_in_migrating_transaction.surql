/**
[test]
reason = "An index is built from the rows as committed, in transactions of its own, so a migration its defining transaction runs and has not committed is not in what the build reads. Such an index is refused before anything is written, a UNIQUE one included, and the transaction commits nothing. Committed first, the migration is read by the build: the index holds the migrated values and refuses the duplicates they make. A migration of another table in the transaction does not refuse an index."

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
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The query was not executed due to a failed transaction"

[[test.results]]
error = "Cannot define an index on table `t` in the transaction that runs migration `a_int`: the index is built from the rows as committed, before the migration. Commit the migration first"

[[test.results]]
error = "Cannot COMMIT: the transaction was aborted due to a prior error"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The query was not executed due to a failed transaction"

[[test.results]]
error = "Cannot define an index on table `t` in the transaction that runs migration `a_int`: the index is built from the rows as committed, before the migration. Commit the migration first"

[[test.results]]
error = "Cannot COMMIT: the transaction was aborted due to a prior error"

[[test.results]]
value = "{  }"

[[test.results]]
value = "['5', '05', '6']"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[t:2]"

[[test.results]]
value = "[1]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Database index `ua` already contains 5, with record `t:1`"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[t:1, t:2]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[t:3]"

*/
DEFINE TABLE t SCHEMAFULL;
DEFINE FIELD a ON t TYPE string;
CREATE t:1 SET a = '5' RETURN NONE;
CREATE t:2 SET a = '05' RETURN NONE;
CREATE t:3 SET a = '6' RETURN NONE;
DEFINE TABLE u SCHEMAFULL;
DEFINE FIELD b ON u TYPE string;
CREATE u:1 SET b = '1' RETURN NONE;

BEGIN;
ALTER FIELD a ON t TYPE int MIGRATE EXISTING ROWS AS a_int USING <int>$value FALLBACK 0;
DEFINE INDEX ia ON t FIELDS a;
COMMIT;
BEGIN;
ALTER FIELD a ON t TYPE int MIGRATE EXISTING ROWS AS a_int USING <int>$value FALLBACK 0;
DEFINE INDEX ua ON t FIELDS a UNIQUE;
COMMIT;
(INFO FOR TABLE t).indexes;
SELECT VALUE a FROM t ORDER BY id;

BEGIN;
ALTER FIELD b ON u TYPE int MIGRATE EXISTING ROWS AS b_int USING <int>$value FALLBACK 0;
DEFINE INDEX ia ON t FIELDS a;
COMMIT;
SELECT VALUE id FROM t WHERE a = '05';
SELECT VALUE b FROM u;
REMOVE INDEX ia ON t;

ALTER FIELD a ON t TYPE int MIGRATE EXISTING ROWS AS a_int USING <int>$value FALLBACK 0;
DEFINE INDEX ua ON t FIELDS a UNIQUE;
DEFINE INDEX ia ON t FIELDS a;
SELECT VALUE id FROM t WHERE a = 5 ORDER BY id;
SELECT VALUE id FROM t WHERE a = '5' ORDER BY id;
SELECT VALUE id FROM t WHERE a >= 6 ORDER BY id;
