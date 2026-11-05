/**
[env]
namespace = true
database = true

[test]
reason = """
A LET no statement may hold, written in a stored clause such as a field's
VALUE, refuses the DEFINE that stores it, wherever the LET stands in the
clause — after a RETURN or in an arm that never runs — so no record is ever
written under it. A valid LET after a RETURN is dead code, and the clause
answers its value.
"""

[[test.results]]
error = "'auth' is a protected variable and cannot be set"

[[test.results]]
value = "[{ id: t:1 }]"

[[test.results]]
error = "'auth' is a protected variable and cannot be set"

[[test.results]]
value = "[{ id: u:1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ h: 1, id: v:1 }]"
*/
DEFINE FIELD f ON t VALUE { RETURN 1; LET $auth = 2 };
CREATE t:1;
DEFINE FIELD g ON u VALUE { IF false { LET $auth = 2 }; 1 };
CREATE u:1;
DEFINE FIELD h ON v VALUE { RETURN 1; LET $z = 2 };
CREATE v:1;
