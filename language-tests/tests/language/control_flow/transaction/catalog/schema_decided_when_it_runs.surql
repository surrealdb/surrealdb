/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
	Where the branch of an IF that defines schema is chosen only when the
	statement runs, the later statement sees the definition the chosen branch
	made, including when the same statement shape runs again after a variable it
	depends on changed.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: t:1, v: 'then' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: t:2, v: 'else' }]"

[[test.results]]
value = "['then', 'else']"
*/

DEFINE TABLE t;
LET $on = true;
IF $on { DEFINE FIELD v ON t VALUE 'then' } ELSE { DEFINE FIELD v ON t VALUE 'else' };
CREATE t:1;

LET $on = false;
IF $on { DEFINE FIELD OVERWRITE v ON t VALUE 'then' } ELSE { DEFINE FIELD OVERWRITE v ON t VALUE 'else' };
CREATE t:2;

SELECT VALUE v FROM t ORDER BY id;
