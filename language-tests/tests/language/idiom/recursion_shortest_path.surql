/**
[test]
reason = "Test recursive shortest path finding through record links"

[[test.results]]
value = "[{ id: a:1, links: [a:2, a:4] }, { id: a:2, links: [a:3] }, { id: a:3, links: [a:5] }, { id: a:4, links: [a:5] }, { id: a:5 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[a:4, a:5]"

[[test.results]]
value = "[a:1, a:4, a:5]"

[env]
timeout = 5000
*/

INSERT [
	{ id: a:1, links: [a:2, a:4] },
	{ id: a:2, links: [a:3] },
	{ id: a:3, links: [a:5] },
	{ id: a:4, links: [a:5] },
	{ id: a:5 },
];

LET $rid = a:5;
a:1.{..+shortest=$rid}.links;
a:1.{..+shortest=a:5+inclusive}.links;
