/**
[test]

[[test.results]]
value = "''"

[[test.results]]
value = "'test'"

[[test.results]]
value = "'this is a test'"

[[test.results]]
value = "'NONENULLtrue11dffffffff-ffff-ffff-ffff-ffffffffffff[]{  }(1, 2)a:b/a/1..2|$a: any| {  }'"

*/
string::concat();
string::concat("test");
string::concat("this", " ", "is", " ", "a", " ", "test");
string::concat(
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
	|$a| {},
);
