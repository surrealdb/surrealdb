/**
[env]
namespace = true
database = true
compatibility = 3
auth = { level = "owner" }

[test]
reason = """
	A declared kind is a hint. A field's kind is enforced on write alone, so
	rows written before a DEFINE FIELD or DEFINE FIELD OVERWRITE hold what
	they held. A declared kind may select an instruction whose fallback
	answers every value the field may hold; it never removes a check, never
	reorders or merges a fallible expression, and never selects an unguarded
	path. The cast of a literal is proven, the read of a field is not. The test
	runs at compatibility level 3, where a retype leaves the rows as they are;
	at level 4 a retype over rows needs MIGRATE EXISTING ROWS.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: n:1, v: '5' }]"

[[test.results]]
value = "[{ id: n:2, v: 'x' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: n:3, v: 7 }]"

[[test.results]]
value = "[n:1, n:2, n:3]"

[[test.results]]
value = "[false, false, false]"

[[test.results]]
error = "Could not cast into `int` using input `'x'`"

[[test.results]]
value = "[true, true, false]"
*/

DEFINE TABLE n SCHEMAFULL;
DEFINE FIELD v ON n TYPE string;
CREATE n:1 SET v = '5';
CREATE n:2 SET v = 'x';
DEFINE FIELD OVERWRITE v ON n TYPE int;
CREATE n:3 SET v = 7;
SELECT VALUE id FROM n WHERE v > 3 ORDER BY id;
SELECT VALUE <int> '5' = v FROM n ORDER BY id;
SELECT VALUE <int> v FROM n:2;
SELECT VALUE type::is_string(v) FROM n ORDER BY id;
