/**
[env]
namespace = true
database = true
timeout = 30000

[test]
reason = "A filter, a map, a builtin's loop and a path broadcast over a bound array read the array in place and leave it unchanged when their body answers a changed element: an extended object, an object without a field, an array with an element pushed. The arrays are longer than one fuel quantum, so each loop head walks the array across yields. A top-level LET, a LET inside a block, a function argument and a literal are each read this way."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[[{ n: 4498, tags: [4498] }, { n: 4499, tags: [4499] }], 2250, { m: 1, n: 4499, tags: [4499] }, [4499, 9], [{ n: 4499, tags: [4499] }], [4499], true, { n: 4499, tags: [4499] }]"

[[test.results]]
value = "true"

[[test.results]]
value = "[4500, { n: 4499 }, 4500, true]"

[[test.results]]
value = "[[{ n: 4498, tags: [4498] }, { n: 4499, tags: [4499] }], { m: 1, n: 4499, tags: [4499] }, [4499, 9], { n: 4499, tags: [4499] }, 4500, true]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[[[{ n: 4499, tags: [4499] }], { m: 1, n: 4499, tags: [4499] }, { n: 4499, tags: [4499] }, 4500], true]"

[[test.results]]
value = "[[{ n: 2 }], [[1, 9], [2, 9]], [{ m: 1, n: 1 }]]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{2, 3}, {2, 3, 4}, {1, 2, 3}]"
*/

-- A top-level LET.
LET $big = array::map(array::range(0, 4500), |$i| { n: $i, tags: [$i] });
LET $copy = array::map(array::range(0, 4500), |$i| { n: $i, tags: [$i] });
RETURN [
	$big[WHERE n >= 4498],
	array::len($big[WHERE n % 2 = 0]),
	$big.map(|$v| object::extend($v, { m: 1 }))[4499],
	array::map($big, |$v| array::push($v.tags, 9))[4499],
	array::filter($big, |$v| $v.n >= 4499),
	($big.tags)[4499],
	$big == $copy,
	$big[4499]
];
RETURN $big == $copy;
-- A path broadcast over a bound array whose elements are changed.
RETURN [
	array::len($big.{ n }),
	($big.{ n })[4499],
	array::len(SELECT * OMIT tags FROM $big),
	$big == $copy
];
-- A LET inside a block.
{
	LET $b = array::map(array::range(0, 4500), |$i| { n: $i, tags: [$i] });
	LET $w = $b[WHERE n >= 4498];
	LET $m = $b.map(|$v| object::extend($v, { m: 1 }));
	LET $p = array::map($b, |$v| array::push($v.tags, 9));
	RETURN [$w, $m[4499], $p[4499], $b[4499], array::len($b), $b == $copy];
};
-- A function argument.
DEFINE FUNCTION fn::loops($b: array) {
	LET $w = $b[WHERE n >= 4499];
	LET $m = $b.map(|$v| object::extend($v, { m: 1 }));
	RETURN [$w, $m[4499], $b[4499], array::len($b)];
};
RETURN [fn::loops($big), $big == $copy];
-- A literal, read from the program.
RETURN [[{ n: 1 }, { n: 2 }][WHERE n > 1], [[1], [2]].map(|$v| array::push($v, 9)), [{ n: 1 }].map(|$v| object::extend($v, { m: 1 }))];
-- A set is copied into the array the loop reads.
LET $set = <set> [3, 1, 2];
RETURN [$set[WHERE $this > 1], $set.map(|$v| $v + 1), $set];
