/**
[test]
reason = "Test recursion with specific depth range bounds"

[[test.results]]
value = "[{ id: a:1, link: a:2 }, { id: a:2, link: a:3 }, { id: a:3, link: a:4 }, { id: a:4, link: a:5 }, { id: a:5 }]"

[[test.results]]
value = "a:3"

[[test.results]]
value = "a:5"

[[test.results]]
value = "a:5"

[env]
timeout = 5000

*/

INSERT [
	{ id: a:1, link: a:2 },
	{ id: a:2, link: a:3 },
	{ id: a:3, link: a:4 },
	{ id: a:4, link: a:5 },
	{ id: a:5 },
];

-- Depth 1 to 2
a:1.{1..2}.link;
-- Depth 2 to 4
a:1.{2..4}.link;
-- Depth from 3 onwards
a:1.{3..}.link;
