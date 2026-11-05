/**
[bench]
reason = "Identity `.*` steps inside a path into a large LET-bound object, read once per loop iteration and once per projected row. `.*` over an array of objects returns its input unchanged, so the path steps over it rather than copying the 2000-object array before reading one field of each element."
run = true
rebuild = false
warmup = "1s"
sample-size = 20
measurement-time = "10s"

[test]
run = false
*/

LET $obj = {
	items: array::map(array::range(0, 2000), |$i| {
		name: <string> $i,
		n: $i,
		tags: ['alpha', 'beta', 'gamma', 'delta', 'epsilon', 'zeta', 'eta', 'theta'],
		meta: { a: $i, b: <string> $i, c: [$i, $i, $i, $i] },
	}),
};

FOR $i IN array::range(0, 200) {
	LET $names = $obj.items.*.name;
};

RETURN array::len((SELECT VALUE $obj.items.*.n FROM array::range(0, 100)));
