/**
[test]

[[test.results]]
value = "[{ id: a:1 }, { id: a:2 }, { id: a:3 }]"

[[test.results]]
value = "[{ bar: true, foo: 123, id: edge:1, in: a:1, out: a:2 }]"

[[test.results]]
value = "[{ bar: true, foo: 123, id: edge:2, in: a:1, out: a:2 }]"

[[test.results]]
value = "[{ bar: true, foo: 456, id: edge:3, in: a:2, out: a:3 }]"

[[test.results]]
value = "[{ bar: false, foo: 456, id: edge:4, in: a:2, out: a:3 }]"

[[test.results]]
value = "[{ edges: [edge:1, edge:2] }, { edges: [edge:3, edge:4] }, { edges: [] }]"

[[test.results]]
value = "[{ edges: [{ foo: 123 }, { foo: 123 }] }, { edges: [{ foo: 456 }, { foo: 456 }] }, { edges: [] }]"

[[test.results]]
value = "[{ edges: [{ foo: 123 }, { foo: 123 }] }, { edges: [] }, { edges: [] }]"

[[test.results]]
value = "[{ edges: [edge:1, edge:2] }, { edges: [] }, { edges: [] }]"

[[test.results]]
value = "[{ edges: [{ bar: true, foo: 123 }, { bar: true, foo: 123 }] }, { edges: [{ bar: true, foo: 456 }, { bar: false, foo: 456 }] }, { edges: [] }]"

[[test.results]]
value = "[{ edges: [{ bar: true, count: 2 }] }, { edges: [{ bar: false, count: 1 }, { bar: true, count: 1 }] }, { edges: [] }]"

[[test.results]]
value = "[{ edges: [edge:1] }, { edges: [edge:3] }, { edges: [] }]"

[[test.results]]
value = "[{ edges: [edge:2] }, { edges: [edge:4] }, { edges: [] }]"

*/
CREATE a:1, a:2, a:3;

RELATE a:1->edge:1->a:2 SET foo = 123, bar = true;
RELATE a:1->edge:2->a:2 SET foo = 123, bar = true;
RELATE a:2->edge:3->a:3 SET foo = 456, bar = true;
RELATE a:2->edge:4->a:3 SET foo = 456, bar = false;

SELECT ->edge AS edges FROM a;
SELECT ->(SELECT foo FROM edge) AS edges FROM a;
SELECT ->(SELECT foo FROM edge WHERE foo = 123) AS edges FROM a;
SELECT ->(edge WHERE foo = 123) AS edges FROM a;
SELECT ->(SELECT foo, bar FROM edge ORDER BY foo DESC) AS edges FROM a;
SELECT ->(SELECT count() as count, bar FROM edge GROUP BY bar) AS edges FROM a;
SELECT ->(edge LIMIT 1) AS edges FROM a;
SELECT ->(edge START 1) AS edges FROM a;
