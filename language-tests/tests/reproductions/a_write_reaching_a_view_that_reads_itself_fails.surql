/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
A view whose sources include itself is maintained by writing a row of its own
table, which it reads, so every row it writes would reach it again. The write
that would maintain it fails with an error naming the view and is undone, and
the datastore goes on serving statements. Defining such a view over rows its
sources already hold fails the same way, as the rows it fills reach it, and
leaves no table behind. Redefining the view over tables that do not read it
lets writes through again.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Cannot maintain the view `s`: it reads itself"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
error = "Cannot maintain the view `t`: it reads itself"

[[test.results]]
value = "['other', 's', 'src']"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: s:1, n: 1 }]"
*/

DEFINE TABLE src SCHEMALESS;
DEFINE TABLE s AS SELECT n + 1 AS n FROM src, s;
CREATE src:1 SET n = 1 RETURN NONE;
SELECT * FROM src;
SELECT * FROM s;

CREATE other:1 SET n = 1 RETURN NONE;
DEFINE TABLE t AS SELECT n + 1 AS n FROM other, t;
object::keys((INFO FOR DB).tables);

DEFINE TABLE OVERWRITE s AS SELECT n FROM src;
CREATE src:1 SET n = 1 RETURN NONE;
SELECT * FROM s;
