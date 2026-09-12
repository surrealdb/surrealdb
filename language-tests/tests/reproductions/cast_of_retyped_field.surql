/**
[env]
auth = { level = "owner" }

[test]
reason = """
	A field's declared kind is enforced on write alone: a value stored before
	`DEFINE FIELD OVERWRITE` retyped the field holds what it held. A cast to
	the declared kind therefore still converts the stored value, on every
	planner strategy, rather than being elided as a cast to the value's own
	kind.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: '7', id: t:1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = '[{ "<int> a": 7 }]'

[[test.results]]
value = "[8]"
*/

DEFINE FIELD a ON t TYPE string;
CREATE t:1 SET a = '7';
DEFINE FIELD OVERWRITE a ON t TYPE int;
SELECT <int> a FROM t;
SELECT VALUE <int> a + 1 FROM t;
