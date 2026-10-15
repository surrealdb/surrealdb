/**
[test]
reason = "A COUNT index whose WHERE guard reads a field no definition declares is an index that reads it: REMOVE UNDECLARED FIELDS is refused until the index is removed, because the count the index holds was judged on values the step clears from every row stored before it. Once the index is removed the step is minted and the rows read without the field."

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
error = "Cannot migrate table `p`: the index `cnt_five` reads a field no definition declares: remove the index first"

[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: p:1, k: 1 }, { id: p:2, k: 2 }]"
*/
DEFINE TABLE p SCHEMALESS;
CREATE p:1 SET a = 5, k = 1 RETURN NONE;
CREATE p:2 SET a = 6, k = 2 RETURN NONE;
DEFINE FIELD k ON p TYPE int;
DEFINE INDEX cnt_five ON p COUNT WHERE a = 5;
-- The refusal leaves the table and its count as they were.
ALTER TABLE p MIGRATE EXISTING ROWS AS drop_undeclared REMOVE UNDECLARED FIELDS;
SELECT count() FROM p WHERE a = 5 GROUP ALL;
-- The way the refusal asks for.
REMOVE INDEX cnt_five ON p;
ALTER TABLE p MIGRATE EXISTING ROWS AS drop_undeclared REMOVE UNDECLARED FIELDS;
SELECT * FROM p ORDER BY id;
