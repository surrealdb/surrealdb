/**
[env]
namespace = true
database = true

[test]
reason = """
`DEFINE FIELD` is not the only statement that changes the value a `COMPUTED`
field derives: the value is coerced to the field's `TYPE`, which `ALTER FIELD`
sets, and a field that is removed derives nothing. While an aggregated view
depends on the field and its table holds a record, each is refused as a new
body is, and the view is removed around the change. A refused statement changes
nothing, so the records keep leaving the groups they joined.

The tables carry an event, so their writes are observed and `COMPUTED` fields
are derived onto the images the views read.
"""

# `t`: a set drops the repeated element of the array `c` derives.
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
value = "[{ c: [1, 1, 2], id: v:[[1, 1, 2]], n: 1 }]"

[[test.results]]
error = "Cannot change the value the computed field `c` of `t` derives: the aggregated view `v` depends on it, and `t` holds records the view has already grouped by the value derived before. Remove `v`, change the field, then define `v` again"

[[test.results]]
value = "[{ c: [1, 1, 2] }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ c: [3], id: v:[[3]], n: 1 }]"

# `r`: the field is removed.
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
error = "Cannot change the value the computed field `c` of `r` derives: the aggregated view `rv` depends on it, and `r` holds records the view has already grouped by the value derived before. Remove `rv`, change the field, then define `rv` again"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ c: 'z', id: rv:['z'], n: 1 }]"

# The view removed first, the field goes.
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
value = "[]"
*/

DEFINE TABLE t SCHEMALESS;
DEFINE FIELD a ON t TYPE option<array<int>>;
DEFINE FIELD c ON t COMPUTED a;
DEFINE EVENT seen ON t THEN {};
DEFINE TABLE v AS SELECT count() AS n, c FROM t GROUP BY c;
CREATE t:1 SET a = [1, 1, 2] RETURN NONE;
SELECT * FROM v;
ALTER FIELD c ON t TYPE set<int>;
SELECT c FROM t:1;
UPDATE t:1 SET a = [3] RETURN NONE;
SELECT * FROM v;

DEFINE TABLE r SCHEMALESS;
DEFINE FIELD a ON r TYPE option<string>;
DEFINE FIELD c ON r COMPUTED a;
DEFINE EVENT seen ON r THEN {};
DEFINE TABLE rv AS SELECT count() AS n, c FROM r GROUP BY c;
CREATE r:1 SET a = 'x' RETURN NONE;
REMOVE FIELD c ON r;
UPDATE r:1 SET a = 'z' RETURN NONE;
SELECT * FROM rv;
BEGIN;
REMOVE TABLE rv;
REMOVE FIELD c ON r;
COMMIT;
DELETE r:1 RETURN NONE;
SELECT * FROM r;
