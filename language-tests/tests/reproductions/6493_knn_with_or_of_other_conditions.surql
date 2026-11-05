/**
[test]
reason = "A vector index query ANDed with a parenthesised OR of other conditions returns the matching rows. Checked on HNSW: the MTREE index the report used no longer exists, and `<|k|>` is refused with a pointer to HNSW."
issue = 6493

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: test:0, point: [0f, 0f, 0f], tags: ['t1', 't'] }, { id: test:1, point: [0f, 1f, 0f], tags: ['t2', 't', 'x'] }, { id: test:2, point: [0f, 1f, 1f], tags: ['t3', 't'] }, { id: test:3, point: [0f, 0f, 1f], tags: ['t4', 't', 'y'] }, { id: test:4, point: [1f, 0f, 0f], tags: ['t5', 't'] }]"

[[test.results]]
value = "[test:0, test:1]"

[[test.results]]
value = "[test:0, test:1]"
*/

DEFINE INDEX test_point ON test FIELDS point HNSW DIMENSION 3;
INSERT INTO test [
	{ id: test:0, tags: ['t1', 't'], point: [ 0f, 0f, 0f ] },
	{ id: test:1, tags: ['t2', 't', 'x'], point: [ 0f, 1f, 0f ] },
	{ id: test:2, tags: ['t3', 't'], point: [ 0f, 1f, 1f ] },
	{ id: test:3, tags: ['t4', 't', 'y'], point: [ 0f, 0f, 1f ] },
	{ id: test:4, tags: ['t5', 't'], point: [ 1f, 0f, 0f ] },
];
SELECT VALUE id FROM test WITH INDEX test_point WHERE (tags CONTAINSALL ['t1'] OR tags CONTAINSALL ['t2']) AND point <|10,40|> [0f, 0f, 1f] ORDER BY id;
SELECT VALUE id FROM test WHERE (tags CONTAINSALL ['t1'] OR tags CONTAINSALL ['t2']) AND point <|10,40|> [0f, 0f, 1f] ORDER BY id;
