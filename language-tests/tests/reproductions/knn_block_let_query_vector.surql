/**
[env]
namespace = true
database = true

[test]
requires = ["diskann"]
reason = """
A KNN whose query vector is only known when the statement runs — a parameter
bound by a `LET` of the same block, a `FOR` loop variable, an array with a
computed element, a path rooted at a bound parameter — finds its neighbours
through the ANN index, as the same query written with a literal vector does:
HNSW, DiskANN, a KNN with an indexed prefilter conjunct, and
`vector::distance::knn()` in the projection. The search evaluates the vector
once, before it reads anything. A brute-force KNN (`<|k,DIST|>`) reads such a
vector the same way, from a table and from a runtime-resolved source. A value
that is not a vector of the index's dimension fails the query, whether it is a
literal or a bound parameter, and an empty one is refused as a vector no
distance ranks a record against.

The plans are pinned in `knn_block_let_query_vector_plan.surql`.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[pts:5]"

[[test.results]]
value = "[pts:5]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[pts:5]"

[[test.results]]
value = "[pts:5]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: out:bf, r: [bpts:3] }, { id: out:dn, r: [dpts:3] }, { id: out:hn, r: [pts:3] }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: dist:dn, r: [{ d: 0f, id: dpts:5 }, { d: 1f, id: dpts:4 }] }, { id: dist:hn, r: [{ d: 0f, id: pts:5 }, { d: 1f, id: pts:4 }] }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[[pts:2, pts:4]]"

[[test.results]]
value = "[pts:5]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[[pts:4]]"

[[test.results]]
error = "Expected `array<number>` but found `'abc'`"

[[test.results]]
error = "Expected `array<number>` but found `'abc'`"

[[test.results]]
error = "Incorrect vector dimension (3). Expected a vector of 2 dimension."

[[test.results]]
error = "Invalid query: The KNN query vector must be a non-empty array of finite numbers: []"

[[test.results]]
error = "Expected `number` but found `'abc'` when coercing element at index 1 of `array<number>`"

[[test.results]]
value = "[bpts:5]"

[[test.results]]
value = "[{ d: 0f, id: bpts:5 }, { d: 1f, id: bpts:4 }]"

[[test.results]]
error = "Expected `array<number>` but found `'abc'`"

[[test.results]]
value = "[pts:5]"

[[test.results]]
value = "[pts:5]"

[[test.results]]
value = "[bpts:5]"

[[test.results]]
value = "[bpts:5]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[[bpts:2]]"
*/

DEFINE FIELD category ON pts TYPE string;
DEFINE INDEX hn ON pts FIELDS point HNSW DIMENSION 2 DIST EUCLIDEAN TYPE F32;
DEFINE INDEX cat ON pts FIELDS category;
DEFINE INDEX dn ON dpts FIELDS point DISKANN DIMENSION 2 DIST EUCLIDEAN TYPE F32;
FOR $i IN 0..6 {
	CREATE type::record('pts', $i) SET point = [<float> $i, 0f], category = IF $i % 2 = 0 { 'a' } ELSE { 'b' };
	CREATE type::record('dpts', $i) SET point = [<float> $i, 0f];
	CREATE type::record('bpts', $i) SET point = [<float> $i, 0f];
};
-- 1. HNSW, the vector bound by the block's only LET
{ LET $q = [5f, 0f]; RETURN SELECT VALUE id FROM pts WHERE point <|1,40|> $q; };
-- 2. HNSW, the vector bound by a later LET
{ LET $a = 1; LET $q = [5f, 0f]; RETURN SELECT VALUE id FROM pts WHERE point <|1,40|> $q; };
-- 3. the block's binding, not the enclosing one, is the query vector
LET $q = [0.2f, 0f];
{ LET $q = [5f, 0f]; RETURN SELECT VALUE id FROM pts WHERE point <|1,40|> $q; };
-- 4. the SELECT as the block's last statement, without RETURN
{ LET $q = [5f, 0f]; SELECT VALUE id FROM pts WHERE point <|1,40|> $q; };
-- 5. a FOR loop variable, over HNSW, DiskANN and brute force
FOR $v IN [[3f, 0f]] { CREATE out:hn SET r = (SELECT VALUE id FROM pts WHERE point <|1,40|> $v); };
FOR $v IN [[3f, 0f]] { CREATE out:dn SET r = (SELECT VALUE id FROM dpts WHERE point <|1,40|> $v); };
FOR $v IN [[3f, 0f]] { CREATE out:bf SET r = (SELECT VALUE id FROM bpts WHERE point <|1,EUCLIDEAN|> $v); };
SELECT id, r FROM out ORDER BY id;
-- 6. DiskANN and HNSW from a FOR variable, with the distances in the projection
FOR $v IN [[5f, 0f]] { CREATE dist:dn SET r = (SELECT id, vector::distance::knn() AS d FROM dpts WHERE point <|2,40|> $v ORDER BY d); };
FOR $v IN [[5f, 0f]] { CREATE dist:hn SET r = (SELECT id, vector::distance::knn() AS d FROM pts WHERE point <|2,40|> $v ORDER BY d); };
SELECT id, r FROM dist ORDER BY id;
-- 7. a prefiltered KNN from a FOR variable: the two nearest of category 'a'
FOR $v IN [[5f, 0f]] { CREATE pre:a SET r = (SELECT VALUE id FROM pts WHERE category = 'a' AND point <|2,40|> $v ORDER BY id); };
SELECT VALUE r FROM pre:a;
-- 8. an array with a computed element
{ LET $x = 5; RETURN SELECT VALUE id FROM pts WHERE point <|1,40|> [<float> $x, 0f]; };
FOR $x IN [4] { CREATE comp:a SET r = (SELECT VALUE id FROM pts WHERE point <|1,40|> [<float> $x, 0f]); };
SELECT VALUE r FROM comp:a;
-- 9. a value that is not a vector, bound and literal
{ LET $q = 'abc'; RETURN SELECT VALUE id FROM pts WHERE point <|1,40|> $q; };
SELECT VALUE id FROM pts WHERE point <|1,40|> 'abc';
-- 10. a vector of the wrong dimension, an empty vector, an element that is not a number
{ LET $q = [1f, 2f, 3f]; RETURN SELECT VALUE id FROM pts WHERE point <|1,40|> $q; };
{ LET $q = []; RETURN SELECT VALUE id FROM pts WHERE point <|1,40|> $q; };
{ LET $q = [1f, 'abc']; RETURN SELECT VALUE id FROM pts WHERE point <|1,40|> $q; };
-- 11. brute force, the vector bound by a LET, with the distance projected, and a bound value that is not a vector
{ LET $q = [5f, 0f]; RETURN SELECT VALUE id FROM bpts WHERE point <|1,EUCLIDEAN|> $q; };
{ LET $q = [5f, 0f]; RETURN SELECT id, vector::distance::knn() AS d FROM bpts WHERE point <|2,EUCLIDEAN|> $q ORDER BY d; };
{ LET $q = 'abc'; RETURN SELECT VALUE id FROM bpts WHERE point <|1,EUCLIDEAN|> $q; };
-- 12. a field of a bound object, an element of a bound array, for HNSW and brute force
{ LET $o = { v: [5f, 0f] }; RETURN SELECT VALUE id FROM pts WHERE point <|1,40|> $o.v; };
{ LET $matrix = [[5f, 0f]]; RETURN SELECT VALUE id FROM pts WHERE point <|1,40|> $matrix[0]; };
{ LET $o = { v: [5f, 0f] }; RETURN SELECT VALUE id FROM bpts WHERE point <|1,EUCLIDEAN|> $o.v; };
{ LET $matrix = [[5f, 0f]]; RETURN SELECT VALUE id FROM bpts WHERE point <|1,EUCLIDEAN|> $matrix[0]; };
-- 13. brute force from a runtime-resolved source, the vector bound by a FOR variable
FOR $v IN [[2f, 0f]] { CREATE dyn:bf SET r = (SELECT VALUE id FROM type::table('bpts') WHERE point <|1,EUCLIDEAN|> $v); };
SELECT VALUE r FROM dyn:bf;
