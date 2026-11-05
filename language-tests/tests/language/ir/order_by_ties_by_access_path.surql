/**
[env]
namespace = true
database = true

[test]
reason = """
Rows whose ORDER BY key ties come back in the order of the access path the statement
reads: the indexed form follows the index order (x ascending), and the WITH NOINDEX form follows the
table scan (id ascending), with and without a LIMIT.
"""

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[t:3, t:2, t:1]"

[[test.results]]
value = "[t:3, t:1, t:2]"

[[test.results]]
value = "[t:3, t:2]"

[[test.results]]
value = "[t:3, t:1]"
*/

CREATE t:1 SET x = 2, y = 1 RETURN NONE;
CREATE t:2 SET x = 1, y = 1 RETURN NONE;
CREATE t:3 SET x = 3, y = 0 RETURN NONE;
DEFINE INDEX ix ON t FIELDS x;
SELECT VALUE id FROM t WHERE x > 0 ORDER BY y;
SELECT VALUE id FROM t WITH NOINDEX WHERE x > 0 ORDER BY y;
SELECT VALUE id FROM t WHERE x > 0 ORDER BY y LIMIT 2;
SELECT VALUE id FROM t WITH NOINDEX WHERE x > 0 ORDER BY y LIMIT 2;
