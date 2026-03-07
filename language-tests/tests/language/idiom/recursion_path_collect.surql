/**
[test]
reason = "Test recursive path with collect instruction"

[[test.results]]
value = "[{ id: a:1, links: [a:2, a:3] }, { id: a:2, links: [a:4] }, { id: a:3, links: [a:5] }, { id: a:4 }, { id: a:5 }]"

[[test.results]]
value = "[a:2, a:3, a:4, a:5]"

[[test.results]]
value = "[[a:2, a:4], [a:3, a:5]]"

*/

INSERT [
	{ id: a:1, links: [a:2, a:3] },
	{ id: a:2, links: [a:4] },
	{ id: a:3, links: [a:5] },
	{ id: a:4 },
	{ id: a:5 },
];

a:1.{..+collect}.links;
a:1.{..+path}.links;
