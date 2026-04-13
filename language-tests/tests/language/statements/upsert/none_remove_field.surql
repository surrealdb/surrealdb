/**
[test]

[[test.results]]
value = "[{ a: 1, b: { c: 1 }, id: test:1 }]"

[[test.results]]
value = "[{ b: {  }, id: test:1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: flex:1, obj: { a: 1 } }]"

[[test.results]]
value = "[{ id: flex:1, obj: {  } }]"

*/
UPSERT test:1 CONTENT {
	a: 1,
	b: {
		c: 1
	}
};

UPSERT test:1 CONTENT {
	a: NONE,
	b: {
		c: NONE,
	}
};

DEFINE TABLE flex SCHEMAFULL;
DEFINE FIELD obj ON flex TYPE object FLEXIBLE;
UPSERT flex:1 CONTENT {
	obj: {
		a: 1
	}
};

UPSERT flex:1 CONTENT {
	obj: {
		a: NONE,
	}
};
