/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "Non-cyclic complex chain mixing all access styles should succeed"

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
value = "NONE"

*/

DEFINE TABLE t_complex SCHEMAFULL;
DEFINE FIELD x ON t_complex TYPE number DEFAULT 1;
DEFINE FIELD a ON t_complex COMPUTED $this.x * 2;
DEFINE FIELD b ON t_complex COMPUTED math::abs($self["a"]);
DEFINE FIELD c ON t_complex COMPUTED IF $this.b > 0 THEN (a + $self.b) ELSE 0 END;
DEFINE FIELD d ON t_complex COMPUTED math::sum([$this.a, b, $self["c"]]);
