/**
[test]
reason = "A predicate that reads a field through a literal record id is folded to a value at plan time so an index can answer it, and the value is the one the per-row read would give: a record of a table with a pending migration step is replayed through the step, so a row stamped before the step answers as the migrated value, not the stored one."

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
value = "[{ id: item:1, n: 5 }]"

[[test.results]]
value = "[{ id: item:1, n: 5 }]"
*/
DEFINE TABLE cfg SCHEMALESS;
DEFINE FIELD n ON cfg TYPE option<string>;
CREATE cfg:1 SET n = '5' RETURN NONE;
ALTER FIELD n ON cfg TYPE option<int> MIGRATE EXISTING ROWS AS n_int USING <int>$value FALLBACK 0;
DEFINE TABLE item SCHEMALESS;
DEFINE INDEX idx_n ON item FIELDS n;
CREATE item:1 SET n = 5 RETURN NONE;
-- The same comparison, once through a parameter the planner substitutes and once with the path written out.
LET $rec = cfg:1;
SELECT * FROM item WHERE n = $rec.n;
SELECT * FROM item WHERE n = (cfg:1).n;
