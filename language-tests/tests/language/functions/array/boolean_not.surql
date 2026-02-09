/**
[test]

[[test.results]]
value = "[true, false, true, false, true, false, true, false, true, false, true, false]"

*/
array::boolean_not([
	false,
	true,
	0,
	1,
	"",
	"1",
	[],
	[1],
	{},
	{a: 1},
	//,
	/1/,
	(1,2),
]);
