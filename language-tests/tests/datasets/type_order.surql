/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "true"

*/

// A parameter with a bunch of types in the order that they need to be sorted.
DEFINE PARAM $type_array VALUE [
	NONE,
	NULL,
	true,
	1,
	1d,
	u"FFFFFFFF-FFFF-FFFF-FFFF-FFFFFFFFFFFF",
	[],
	{},
	(1,2),
	a:b,
	/a/,
	1..2,
];
$type_array == array::sort($type_array)
