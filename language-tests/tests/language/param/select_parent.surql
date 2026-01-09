/**
[test]

[[test.results]]
value = "[{ a: 1, id: foo:1, this: 2 }]"

[[test.results]]
value = "[{ a: 3, id: foo:2, this: 4 }]"

[[test.results]]
value = "[{ value: [{ parent: { a: 1, id: foo:1, this: 2 }, this: { a: 3, id: foo:2, this: 4 } }] }]"
*/

create foo:1 CONTENT { a: 1, this: 2};
create foo:2 CONTENT { a: 3, this: 4};
SELECT (SELECT $parent as parent, $this as this from foo:2) as value from foo:1;
