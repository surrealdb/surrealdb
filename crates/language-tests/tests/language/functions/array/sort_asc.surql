/**
[test]

[[test.results]]
value = "[]"

[[test.results]]
error = "Incorrect arguments for function array::sort::asc(). Expected 1 argument."

[[test.results]]
value = "[1, 2, 3, 4, 4, 'text']"

[[test.results]]
value = "[1, 2, 3, 4, 4, 'text']"

[[test.results]]
value = "[1, 2, 3, 4, 4, 'text']"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[NONE, NULL, true, 1, 1d, u'ffffffff-ffff-ffff-ffff-ffffffffffff', [], {  }, (1, 2), a:b, |a:1..2|, /a/, 1..2, |$a: any| {  }]"

*/

array::sort::asc([]);
array::sort::asc(3, false);
array::sort::asc([4,2,"text",1,3,4]);
array::sort::asc([4,2,"text",1,3,4]);
array::sort::asc([4,2,"text",1,3,4]);

let $type_array = [
	|$a: any| {  },
	1..2,
	/a/,
	|a:1..2|,
	a:b,
	(1, 2),
	{  },
	[],
	u'ffffffff-ffff-ffff-ffff-ffffffffffff',
	1d,
	1,
	true,
	NULL,
	NONE
];
array::sort::asc($type_array);
