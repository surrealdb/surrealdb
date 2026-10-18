/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
CURRENT BEHAVIOUR PINNED, NOT THE ISSUE'S EXPECTATION: literal kinds ([int], [int, string], { a: int }) validate but do not convert their members, so [1.0] stays [1f]. Changing it also changes tuple-typed record ids, whose float elements language/write/unnamed_tuple_id_writes_unchanged pins as kept.
"""
issue = 6697

[[test.results]]
value = "NONE"

[[test.results]]
value = "[1f]"

[[test.results]]
value = "'float'"

[[test.results]]
value = "NONE"

[[test.results]]
value = "'int'"

[[test.results]]
value = "NONE"

[[test.results]]
value = "'float'"

[[test.results]]
value = "NONE"

[[test.results]]
value = "'float'"

*/
LET $v: [int] = [1.0];
$v;
type::of($v[0]);
LET $w: array<int> = [1.0];
type::of($w[0]);
LET $x: [int, string] = [1.0, 's'];
type::of($x[0]);
LET $o: { a: int } = { a: 1.0 };
type::of($o.a);
