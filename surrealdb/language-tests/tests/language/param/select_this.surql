/**
[test]

[[test.results]]
value = "[{ a: 1, id: foo:1, this: 2 }]"

[[test.results]]
value = "[{ this: { a: 1, id: foo:1, this: 2 } }]"

[[test.results]]
value = "[{ this: { a: 1, id: foo:1, this: 2 } }]"
*/

create foo:1 CONTENT { a: 1, this: 2};
SELECT ($this) from foo:1;
SELECT $this from foo:1;
