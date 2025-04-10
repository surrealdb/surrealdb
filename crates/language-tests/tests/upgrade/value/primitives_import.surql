/**
[test]

[[test.results]]
value = '''[{ data: [NONE, NULL, true, false, 10, 10f, 10dec, 1h, d'2012-01-01T08:00:00Z', u'00000000-0000-0000-0000-000000000000', [1, 2, 3], { a: 1 }, (1, 2), encoding::base64::decode("MjMyMw"), a:b, |a:1..100|, /a/, 0..10], id: test:values }]'''


*/
CREATE test:values SET data = [
	NONE,
	NULL,
	true,
	false,
	10,
	10f,
	<decimal> 10,
	1h,
	d"2012-01-01T08:00:00Z",
	u"00000000-0000-0000-0000-000000000000",
	[1,2,3],
	{a: 1},
	(1,2),
	encoding::base64::decode("MjMyMw"),
	type::thing('a','b'),
	|a:1..100|,
	/a/,
	0..10,
];
