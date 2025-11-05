/**
[test]

[[test.results]]
value = "NONE"
[[test.results]]
value = "NONE"
[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: test:1, b: {}, c: [NONE, {}, {}] }]"
[[test.results]]
value = "[{ id: test:2, a: 123, b: { key: 123 }, c: [NONE, {}, { key: 123 }] }]"

*/

DEFINE FIELD a ON test TYPE option<number>;
DEFINE FIELD b ON test TYPE { key: option<number> };
DEFINE FIELD c ON test TYPE array<option<{ key: option<number> }>>;

CREATE test:1 CONTENT {
	a: NONE,
	b: { key: NONE },
	c: [NONE, {}, { key: NONE }]
};

CREATE test:2 CONTENT {
	a: 123,
	b: { key: 123 },
	c: [NONE, {}, { key: 123 }]
};
