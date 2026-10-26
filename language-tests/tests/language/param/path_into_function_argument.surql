/**
[env]
namespace = true
database = true

[test]
reason = "A path rooted at a user-defined function's argument reads the argument the call bound, alongside a LET in the body, a LET in the body that reuses the argument's name, and an outer LET of the same name the body cannot see."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[2, 2, 3]"

[[test.results]]
value = "8"

[[test.results]]
value = "7"

[[test.results]]
value = "300"

[[test.results]]
value = "[[100, 100, 200], [200, 200, 300]]"
*/

DEFINE FUNCTION fn::pick($xs: array, $i: int) {
	LET $ys = $xs;
	RETURN [$xs[$i], $ys[$i], $xs[$i + 1]];
};
DEFINE FUNCTION fn::rebind($xs: array) {
	LET $xs = [9, 8];
	RETURN $xs[1];
};
DEFINE FUNCTION fn::nested($o: object) {
	RETURN $o.a.b[0];
};

LET $xs = [100, 200, 300];
fn::pick([1, 2, 3], 1);
fn::rebind([1, 2]);
fn::nested({ a: { b: [7] } });
$xs[2];
SELECT VALUE fn::pick($xs, $this) FROM [0, 1];
