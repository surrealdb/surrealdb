/**
[test]
reason = "Test recursive idiom path elimination with nested destructuring"

[[test.results]]
value = "[{ id: a:1, links: [a:2, a:3], name: 'One' }, { id: a:2, links: [a:3], name: 'Two' }, { id: a:3, links: [], name: 'Three' }]"

[[test.results]]
value = "{ links: [a:2, a:3], name: 'One' }"

[[test.results]]
value = "{ links: [{ links: [a:3], name: 'Two' }, { links: [], name: 'Three' }], name: 'One' }"

[[test.results]]
value = "{ links: [{ links: [{ links: [], name: 'Three' }], name: 'Two' }], name: 'One' }"

[[test.results]]
value = "NONE"

[env]
timeout = 5000
*/

INSERT [
	{ id: a:1, name: 'One',   links: [a:2, a:3] },
	{ id: a:2, name: 'Two',   links: [a:3] },
	{ id: a:3, name: 'Three', links: [] },
];

a:1.{1}.{ name, links: links.@ };
a:1.{2}.{ name, links: links.@ };
a:1.{3}.{ name, links: links.@ };
a:1.{4}.{ name, links: links.@ };
