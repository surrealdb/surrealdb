/**
[test]
reason = "A conditional UPSERT creates its guaranteed record only when nothing matched, and what the statement returns does not enter into it. `RETURN NONE` discards every row's output by construction, so the fallback is driven by whether the statement matched a record rather than by whether it produced results: a statement that matched creates nothing more, including when its data clause leaves the matched record unchanged and no write happens at all; a statement that matched nothing still creates exactly one record. `RETURN BEFORE` holds the same contract for a statement that does produce results. The sibling contract under `OPTION IMPORT`, where the fallback is suppressed outright, is held by `upsert_where_option_import_phantom.surql`."

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: a:1, n: 1 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: b:1, n: 1 }]"

[[test.results]]
value = "[{ id: b:1, n: 1 }]"

[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: c:1, n: 1 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: d:1, n: 1 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ count: 2 }]"
*/

DEFINE TABLE a SCHEMALESS;
CREATE a:1 SET n = 1;
-- The condition matches a:1, which is updated; nothing else is created.
UPSERT a SET tag = 'x' WHERE n = 1 RETURN NONE;
SELECT count() FROM a GROUP ALL;

DEFINE TABLE b SCHEMALESS;
CREATE b:1 SET n = 1;
-- The same statement returning rows creates nothing extra either.
UPSERT b SET tag = 'x' WHERE n = 1 RETURN BEFORE;
SELECT count() FROM b GROUP ALL;

DEFINE TABLE c SCHEMALESS;
CREATE c:1 SET n = 1;
-- The data clause leaves c:1 as it was, so no KV write happens — but the
-- record matched, which is what the fallback asks.
UPSERT c SET n = 1 WHERE n = 1 RETURN NONE;
SELECT count() FROM c GROUP ALL;

DEFINE TABLE d SCHEMALESS;
CREATE d:1 SET n = 1;
-- Nothing matches, so the fallback still creates its one record.
UPSERT d SET tag = 'y' WHERE n = 99 RETURN NONE;
SELECT count() FROM d GROUP ALL;
