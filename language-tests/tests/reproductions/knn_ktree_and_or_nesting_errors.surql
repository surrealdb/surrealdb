/**
[env]
planner-strategy = ["all-ro"]

[test]
reason = "KNN diagnostics: the legacy `<|k|>` (KTree) operator now emits a distinct, actionable error pointing at the supported variants, and KNN nested under OR/NOT emits a separate error explaining the placement rule. Top-level-AND queries that combine an HNSW or brute-force KNN with an OR sub-clause keep working."

# DEFINE INDEX
[[test.results]]
value = "NONE"

# INSERT (8 rows)
[[test.results]]
value = "[{ id: test:0, point: [0f, 0f, 0f], tags: ['t1', 't'] }, { id: test:1, point: [0f, 1f, 0f], tags: ['t2', 't', 'x'] }, { id: test:2, point: [0f, 1f, 1f], tags: ['t3', 't'] }, { id: test:3, point: [0f, 0f, 1f], tags: ['t4', 't', 'y'] }, { id: test:4, point: [1f, 0f, 0f], tags: ['t5', 't'] }, { id: test:5, point: [1f, 1f, 0f], tags: ['t6', 't', 'z'] }, { id: test:6, point: [1f, 1f, 1f], tags: ['t7', 't'] }, { id: test:7, point: [1f, 0f, 1f], tags: ['t8', 't'] }]"

# LET $pt = ...
[[test.results]]
value = "NONE"

# 1. KTree `<|10|>` inside AND-of-OR: dedicated "variant unsupported" error.
[[test.results]]
error = "Invalid query: The `<|k|>` KNN operator (KTree / M-Tree) is no longer supported. Use `<|k, EF|>` against an HNSW index (e.g. `DEFINE INDEX … HNSW DIMENSION N`), or `<|k, DISTANCE|>` for a brute-force KNN with an explicit distance metric."

# 2. KTree `<|10|>` in pure AND chain: same error (not an OR/NOT issue at all).
[[test.results]]
error = "Invalid query: The `<|k|>` KNN operator (KTree / M-Tree) is no longer supported. Use `<|k, EF|>` against an HNSW index (e.g. `DEFINE INDEX … HNSW DIMENSION N`), or `<|k, DISTANCE|>` for a brute-force KNN with an explicit distance metric."

# 3. HNSW `<|10,40|>` inside AND-of-OR: WORKS — the AND-of-OR pattern is supported as long as the KNN itself sits at the top-level AND. Ordered by id (rather than similarity DESC) because test:0's zero vector produces a NaN cosine and NaN comparisons are unordered.
[[test.results]]
value = "[{ id: test:0, tags: ['t1', 't'] }, { id: test:1, tags: ['t2', 't', 'x'] }]"

# 4. Brute-force `<|10,EUCLIDEAN|>` inside AND-of-OR: WORKS — same shape, different KNN variant.
[[test.results]]
value = "[{ id: test:0, tags: ['t1', 't'] }, { id: test:1, tags: ['t2', 't', 'x'] }]"

# 5. HNSW KNN nested directly under OR: distinct "placement" error.
[[test.results]]
error = "Invalid query: KNN operators must appear at the top level of the WHERE clause (joined with AND); nesting `<|k, …|>` inside OR or NOT is not supported."
*/

DEFINE INDEX OVERWRITE test_point ON test FIELDS point HNSW DIMENSION 3;

INSERT INTO test [
	{ id: test:0, tags: ['t1', 't'], point: [ 0f, 0f, 0f ] },
	{ id: test:1, tags: ['t2', 't', 'x'], point: [ 0f, 1f, 0f ] },
	{ id: test:2, tags: ['t3', 't'], point: [ 0f, 1f, 1f ] },
	{ id: test:3, tags: ['t4', 't', 'y'], point: [ 0f, 0f, 1f ] },
	{ id: test:4, tags: ['t5', 't'], point: [ 1f, 0f, 0f ] },
	{ id: test:5, tags: ['t6', 't', 'z'], point: [ 1f, 1f, 0f ] },
	{ id: test:6, tags: ['t7', 't'], point: [ 1f, 1f, 1f ] },
	{ id: test:7, tags: ['t8', 't'], point: [ 1f, 0f, 1f ] },
];

LET $pt = [0f, 0f, 1f];

-- 1. KTree (`<|10|>`) inside an AND with an OR — variant unsupported.
SELECT
	id, tags, point, vector::similarity::cosine(point, $pt) AS similarity
FROM test
WITH INDEX test_point
WHERE
	(tags CONTAINSALL ['t1'] OR tags CONTAINSALL ['t2']) AND point <|10|> $pt
ORDER BY similarity DESC;

-- 2. KTree (`<|10|>`) in a pure AND chain — variant unsupported.
SELECT
	id, tags, point, vector::similarity::cosine(point, $pt) AS similarity
FROM test
WITH INDEX test_point
WHERE
	tags CONTAINSALL ['t1'] AND tags CONTAINSANY ['t', 't2'] AND point <|10|> $pt
ORDER BY similarity DESC;

-- 3. HNSW Approximate (`<|10,40|>`) inside an AND with an OR — supported. Project only id+tags
-- and order by id; we just want to confirm the right rows survive the WHERE, not pin the
-- similarity value (test:0's [0,0,0] vector produces a NaN cosine which sorts non-deterministically).
SELECT id, tags FROM test
WITH INDEX test_point
WHERE
	(tags CONTAINSALL ['t1'] OR tags CONTAINSALL ['t2']) AND point <|10,40|> $pt
ORDER BY id;

-- 4. Brute-force K with explicit distance (`<|10,EUCLIDEAN|>`) inside an AND with an OR — supported.
SELECT id, tags FROM test
WITH INDEX test_point
WHERE
	(tags CONTAINSALL ['t1'] OR tags CONTAINSALL ['t2']) AND point <|10,EUCLIDEAN|> $pt
ORDER BY id;

-- 5. HNSW Approximate nested directly under an OR — placement error.
SELECT
	id, tags, point, vector::similarity::cosine(point, $pt) AS similarity
FROM test
WITH INDEX test_point
WHERE
	point <|10,40|> $pt OR tags CONTAINSALL ['t1']
ORDER BY similarity DESC;
