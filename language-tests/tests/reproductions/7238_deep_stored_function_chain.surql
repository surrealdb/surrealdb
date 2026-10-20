/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
A finite chain of stored functions forty calls deep returns its value rather
than overflowing the worker stack, and a call chain that recurses without end
fails with the computation depth error.
"""
issue = 7238

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

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "40"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Reached excessive computation depth due to functions, subqueries, or computed values"

[[test.results]]
value = "140"
*/
DEFINE FUNCTION fn::f0($x: int) { LET $y = $x + 1; FOR $i IN [1] { LET $z = $i; }; RETURN fn::f1($y); };
DEFINE FUNCTION fn::f1($x: int) { LET $y = $x + 1; FOR $i IN [1] { LET $z = $i; }; RETURN fn::f2($y); };
DEFINE FUNCTION fn::f2($x: int) { LET $y = $x + 1; FOR $i IN [1] { LET $z = $i; }; RETURN fn::f3($y); };
DEFINE FUNCTION fn::f3($x: int) { LET $y = $x + 1; FOR $i IN [1] { LET $z = $i; }; RETURN fn::f4($y); };
DEFINE FUNCTION fn::f4($x: int) { LET $y = $x + 1; FOR $i IN [1] { LET $z = $i; }; RETURN fn::f5($y); };
DEFINE FUNCTION fn::f5($x: int) { LET $y = $x + 1; FOR $i IN [1] { LET $z = $i; }; RETURN fn::f6($y); };
DEFINE FUNCTION fn::f6($x: int) { LET $y = $x + 1; FOR $i IN [1] { LET $z = $i; }; RETURN fn::f7($y); };
DEFINE FUNCTION fn::f7($x: int) { LET $y = $x + 1; FOR $i IN [1] { LET $z = $i; }; RETURN fn::f8($y); };
DEFINE FUNCTION fn::f8($x: int) { LET $y = $x + 1; FOR $i IN [1] { LET $z = $i; }; RETURN fn::f9($y); };
DEFINE FUNCTION fn::f9($x: int) { LET $y = $x + 1; FOR $i IN [1] { LET $z = $i; }; RETURN fn::f10($y); };
DEFINE FUNCTION fn::f10($x: int) { LET $y = $x + 1; FOR $i IN [1] { LET $z = $i; }; RETURN fn::f11($y); };
DEFINE FUNCTION fn::f11($x: int) { LET $y = $x + 1; FOR $i IN [1] { LET $z = $i; }; RETURN fn::f12($y); };
DEFINE FUNCTION fn::f12($x: int) { LET $y = $x + 1; FOR $i IN [1] { LET $z = $i; }; RETURN fn::f13($y); };
DEFINE FUNCTION fn::f13($x: int) { LET $y = $x + 1; FOR $i IN [1] { LET $z = $i; }; RETURN fn::f14($y); };
DEFINE FUNCTION fn::f14($x: int) { LET $y = $x + 1; FOR $i IN [1] { LET $z = $i; }; RETURN fn::f15($y); };
DEFINE FUNCTION fn::f15($x: int) { LET $y = $x + 1; FOR $i IN [1] { LET $z = $i; }; RETURN fn::f16($y); };
DEFINE FUNCTION fn::f16($x: int) { LET $y = $x + 1; FOR $i IN [1] { LET $z = $i; }; RETURN fn::f17($y); };
DEFINE FUNCTION fn::f17($x: int) { LET $y = $x + 1; FOR $i IN [1] { LET $z = $i; }; RETURN fn::f18($y); };
DEFINE FUNCTION fn::f18($x: int) { LET $y = $x + 1; FOR $i IN [1] { LET $z = $i; }; RETURN fn::f19($y); };
DEFINE FUNCTION fn::f19($x: int) { LET $y = $x + 1; FOR $i IN [1] { LET $z = $i; }; RETURN fn::f20($y); };
DEFINE FUNCTION fn::f20($x: int) { LET $y = $x + 1; FOR $i IN [1] { LET $z = $i; }; RETURN fn::f21($y); };
DEFINE FUNCTION fn::f21($x: int) { LET $y = $x + 1; FOR $i IN [1] { LET $z = $i; }; RETURN fn::f22($y); };
DEFINE FUNCTION fn::f22($x: int) { LET $y = $x + 1; FOR $i IN [1] { LET $z = $i; }; RETURN fn::f23($y); };
DEFINE FUNCTION fn::f23($x: int) { LET $y = $x + 1; FOR $i IN [1] { LET $z = $i; }; RETURN fn::f24($y); };
DEFINE FUNCTION fn::f24($x: int) { LET $y = $x + 1; FOR $i IN [1] { LET $z = $i; }; RETURN fn::f25($y); };
DEFINE FUNCTION fn::f25($x: int) { LET $y = $x + 1; FOR $i IN [1] { LET $z = $i; }; RETURN fn::f26($y); };
DEFINE FUNCTION fn::f26($x: int) { LET $y = $x + 1; FOR $i IN [1] { LET $z = $i; }; RETURN fn::f27($y); };
DEFINE FUNCTION fn::f27($x: int) { LET $y = $x + 1; FOR $i IN [1] { LET $z = $i; }; RETURN fn::f28($y); };
DEFINE FUNCTION fn::f28($x: int) { LET $y = $x + 1; FOR $i IN [1] { LET $z = $i; }; RETURN fn::f29($y); };
DEFINE FUNCTION fn::f29($x: int) { LET $y = $x + 1; FOR $i IN [1] { LET $z = $i; }; RETURN fn::f30($y); };
DEFINE FUNCTION fn::f30($x: int) { LET $y = $x + 1; FOR $i IN [1] { LET $z = $i; }; RETURN fn::f31($y); };
DEFINE FUNCTION fn::f31($x: int) { LET $y = $x + 1; FOR $i IN [1] { LET $z = $i; }; RETURN fn::f32($y); };
DEFINE FUNCTION fn::f32($x: int) { LET $y = $x + 1; FOR $i IN [1] { LET $z = $i; }; RETURN fn::f33($y); };
DEFINE FUNCTION fn::f33($x: int) { LET $y = $x + 1; FOR $i IN [1] { LET $z = $i; }; RETURN fn::f34($y); };
DEFINE FUNCTION fn::f34($x: int) { LET $y = $x + 1; FOR $i IN [1] { LET $z = $i; }; RETURN fn::f35($y); };
DEFINE FUNCTION fn::f35($x: int) { LET $y = $x + 1; FOR $i IN [1] { LET $z = $i; }; RETURN fn::f36($y); };
DEFINE FUNCTION fn::f36($x: int) { LET $y = $x + 1; FOR $i IN [1] { LET $z = $i; }; RETURN fn::f37($y); };
DEFINE FUNCTION fn::f37($x: int) { LET $y = $x + 1; FOR $i IN [1] { LET $z = $i; }; RETURN fn::f38($y); };
DEFINE FUNCTION fn::f38($x: int) { LET $y = $x + 1; FOR $i IN [1] { LET $z = $i; }; RETURN fn::f39($y); };
DEFINE FUNCTION fn::f39($x: int) { RETURN $x + 1; };
RETURN fn::f0(0);
DEFINE FUNCTION fn::forever($x: int) { RETURN fn::forever($x + 1); };
RETURN fn::forever(0);
RETURN fn::f0(100);
