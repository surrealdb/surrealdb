/**
[test]
reason = "Test recursive idiom with repeat recurse and nested destructuring"

[[test.results]]
value = "[{ id: a:1, links: { a: [a:2, a:3] }, name: 'One' }, { id: a:2, links: { a: [a:3] }, name: 'Two' }, { id: a:3, links: { a: [] }, name: 'Three' }]"

[[test.results]]
value = "{ links: { a: [a:2, a:3] }, name: 'One' }"

[[test.results]]
value = "{ links: { a: [{ links: { a: [a:3] }, name: 'Two' }, { links: { a: [] }, name: 'Three' }] }, name: 'One' }"

[[test.results]]
value = "{ links: { a: [{ links: { a: [{ links: { a: [] }, name: 'Three' }] }, name: 'Two' }] }, name: 'One' }"

[[test.results]]
value = "NONE"

[env]
timeout = 5000
*/

INSERT [
	{ id: a:1, name: 'One',   links: { a: [a:2, a:3] } },
	{ id: a:2, name: 'Two',   links: { a: [a:3] } },
	{ id: a:3, name: 'Three', links: { a: [] } },
];

a:1.{1}.{ name, links.{ a: a.@ } };
a:1.{2}.{ name, links.{ a: a.@ } };
a:1.{3}.{ name, links.{ a: a.@ } };
a:1.{4}.{ name, links.{ a: a.@ } };
