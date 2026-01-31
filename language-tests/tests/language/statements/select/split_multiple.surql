/**
[test]

# 0: Create a single record with multiple arrays
[[test.results]]
value = "[{ a: [1, 2], b: [3, 4], c: [5, 6], id: t:1 }]"

# 1, 2, 3: Baseline results, splitting on a single field.
[[test.results]]
value = "[{ a: 1, b: [3, 4], c: [5, 6], id: t:1 }, { a: 2, b: [3, 4], c: [5, 6], id: t:1 }]"
[[test.results]]
value = "[{ a: [1, 2], b: 3, c: [5, 6], id: t:1 }, { a: [1, 2], b: 4, c: [5, 6], id: t:1 }]"
[[test.results]]
value = "[{ a: [1, 2], b: [3, 4], c: 5, id: t:1 }, { a: [1, 2], b: [3, 4], c: 6, id: t:1 }]"

# 4, 5: Split on a and b in different orders
[[test.results]]
value = "[{ a: 1, b: 3, c: [5, 6], id: t:1 }, { a: 1, b: 4, c: [5, 6], id: t:1 }, { a: 2, b: 3, c: [5, 6], id: t:1 }, { a: 2, b: 4, c: [5, 6], id: t:1 }]"
[[test.results]]
value = "[{ a: 1, b: 3, c: [5, 6], id: t:1 }, { a: 2, b: 3, c: [5, 6], id: t:1 }, { a: 1, b: 4, c: [5, 6], id: t:1 }, { a: 2, b: 4, c: [5, 6], id: t:1 }]"

# 6: Split on all three fields
[[test.results]]
value = "[{ a: 1, b: 3, c: 5, id: t:1 }, { a: 1, b: 3, c: 6, id: t:1 }, { a: 1, b: 4, c: 5, id: t:1 }, { a: 1, b: 4, c: 6, id: t:1 }, { a: 2, b: 3, c: 5, id: t:1 }, { a: 2, b: 3, c: 6, id: t:1 }, { a: 2, b: 4, c: 5, id: t:1 }, { a: 2, b: 4, c: 6, id: t:1 }]"

*/
// 0: Create a single record with multiple arrays
CREATE t:1 SET a = [1, 2], b = [3, 4], c = [5, 6];
// 1, 2, 3: Baseline results (splitting on a single field is covered thoroughly in other tests).
SELECT * FROM t SPLIT a;
SELECT * FROM t SPLIT b;
SELECT * FROM t SPLIT c;

// 4, 5: Split on a and b in different orders
SELECT * FROM t SPLIT a, b;
SELECT * FROM t SPLIT b, a;

// 6: Split on three fields
SELECT * FROM t SPLIT a, b, c;
