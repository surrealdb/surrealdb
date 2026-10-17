/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
A view whose sources include itself, or a view that reads it directly or
through other views, is refused: each row it wrote would reach it again through
its own maintenance, and a projection that changes the row never settles. A
refused definition leaves the table as it was, still registered where it was.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
error = "Invalid query: Cannot define the view `s` over itself"

[[test.results]]
value = "['src']"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Invalid query: Cannot define the view `v1` over `v3`, a view that reads `v1`"

[[test.results]]
error = "Invalid query: Cannot define the view `v1` over itself"

[[test.results]]
value = "'DEFINE TABLE v1 TYPE ANY SCHEMALESS AS SELECT n FROM src PERMISSIONS NONE'"

[[test.results]]
value = "['v2']"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: v3:1, n: 2 }, { id: v3:2, n: 3 }]"
*/

DEFINE TABLE src SCHEMALESS;
CREATE src:1 SET n = 1 RETURN NONE;
DEFINE TABLE s AS SELECT (n ?? 0) + 1 AS n FROM src, s;
object::keys((INFO FOR DB).tables);
DEFINE TABLE v1 AS SELECT n FROM src;
DEFINE TABLE v2 AS SELECT n + 1 AS n FROM v1;
DEFINE TABLE v3 AS SELECT n FROM v2;
DEFINE TABLE OVERWRITE v1 AS SELECT n FROM src, v3;
DEFINE TABLE OVERWRITE v1 AS SELECT n FROM v1;
(INFO FOR DB).tables.v1;
object::keys((INFO FOR TABLE v1).tables);
CREATE src:2 SET n = 2 RETURN NONE;
SELECT * FROM v3 ORDER BY id;
