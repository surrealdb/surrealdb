/**
[test]
reason = "This test proves that $this is fixed at the time when the query is executed, and does not change throughout the various assignments"

[[test.results]]
value = "[{ a: { id: test:1 }, b: { id: test:1 }, c: { id: test:1 }, id: test:1 }]"

*/
UPSERT test:1 SET a = $this, b = $this, c = $this;