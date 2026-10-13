/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
Register #4: a `SELECT` that names a table which is not defined raises "The
table '...' does not exist" for a caller whose reads are not permission-checked,
whatever its `WHERE` says. A predicate that is provably false does not turn the
read into an empty answer, because whether the table exists is decided when the
source is opened and not by the rows that would survive. A table that is defined
answers the empty set under the same predicate.

The expectation is the register's answer. It holds once `SELECT` plans through
the access chooser, which sees the table the false filter stands over.
"""

[[test.results]]
value = "NONE"

[[test.results]]
error = "The table 'nosuch' does not exist"

[[test.results]]
error = "The table 'nosuch' does not exist"

[[test.results]]
error = "The table 'nosuch' does not exist"

[[test.results]]
error = "The table 'nosuch' does not exist"

[[test.results]]
value = "[]"
*/
DEFINE TABLE t SCHEMALESS;
SELECT * FROM nosuch;
SELECT * FROM nosuch WHERE false;
SELECT * FROM nosuch WHERE 1 = 2;
SELECT * FROM nosuch WHERE 1 IN [];
SELECT * FROM t WHERE false;
