/**
[test]
reason = "A KNN query vector bound as a parameter is planned from the parameter's value, as a literal vector is: through an HNSW and a DiskANN index, by brute force, from a DEFINE PARAM, as integers, and when the same parameter is also read elsewhere in the condition; and a name a function, closure, FOR or block LET binds is that binding's value, not the statement parameter it shadows. An ANN search whose vector a closure or a FOR binds is in knn_index_query_vector_bound_per_call.surql."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ d: 0.20000000707805143f, id: pts:0 }, { d: 0.8000000283122057f, id: pts:1 }, { d: 1.7999999364217112f, id: pts:2 }]"

[[test.results]]
value = "[{ d: 0.20000000707805143f, id: dpts:0 }, { d: 0.8000000283122057f, id: dpts:1 }, { d: 1.7999999364217112f, id: dpts:2 }]"

[[test.results]]
value = "[{ d: 0.2f, id: bpts:0 }, { d: 0.8f, id: bpts:1 }, { d: 1.8f, id: bpts:2 }]"

[[test.results]]
value = "[{ id: pts:0 }, { id: pts:1 }, { id: pts:2 }]"

[[test.results]]
value = "[{ id: pts:0 }, { id: pts:1 }, { id: pts:2 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: pts:5 }, { id: pts:4 }]"

[[test.results]]
value = "[{ id: bpts:4 }, { id: bpts:5 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: pts:4 }, { id: pts:3 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[pts:5]"

[[test.results]]
value = "[[bpts:5], [bpts:3]]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ bf: [bpts:3], q: [3f, 0f] }, { bf: [bpts:5], q: [5f, 0f] }]"

[[test.results]]
value = "[bpts:5]"

[[test.results]]
value = "[pts:0]"

[[test.results]]
value = "[bpts:5]"

*/

DEFINE INDEX hn ON pts FIELDS point HNSW DIMENSION 2;
DEFINE INDEX dk ON dpts FIELDS point DISKANN DIMENSION 2;
FOR $i IN 0..6 {
	CREATE type::record('pts', $i) SET point = [<float> $i, 0f];
	CREATE type::record('dpts', $i) SET point = [<float> $i, 0f];
	CREATE type::record('bpts', $i) SET point = [<float> $i, 0f];
};
LET $q = [0.2f, 0f];

SELECT id, vector::distance::knn() AS d FROM pts WHERE point <|3,40|> $q ORDER BY d;
SELECT id, vector::distance::knn() AS d FROM dpts WHERE point <|3,40|> $q ORDER BY d;
SELECT id, vector::distance::knn() AS d FROM bpts WHERE point <|3,EUCLIDEAN|> $q ORDER BY d;
-- The same parameter read elsewhere in the condition keeps its value there.
SELECT id FROM pts WHERE point <|3,40|> $q AND array::len($q) = 2;
SELECT id FROM pts WHERE point <|3,40|> $q AND point != $q AND array::first($q) < 1;
-- Integers.
LET $qi = [5, 0];
SELECT id FROM pts WHERE point <|2,40|> $qi;
SELECT id FROM bpts WHERE point <|2,EUCLIDEAN|> $qi ORDER BY id;
-- A DEFINE PARAM.
DEFINE PARAM $dq VALUE [4f, 0f];
SELECT id FROM pts WHERE point <|2,40|> $dq;
-- A name a function argument, a closure argument, a FOR variable or a block
-- LET binds is that binding's value, not the statement parameter it shadows.
DEFINE FUNCTION fn::nn($q: array) { RETURN SELECT VALUE id FROM pts WHERE point <|1,40|> $q; };
RETURN fn::nn([5f, 0f]);
RETURN [[5f, 0f], [3f, 0f]].map(|$q| SELECT VALUE id FROM bpts WHERE point <|1,EUCLIDEAN|> $q);
FOR $q IN [[5f, 0f], [3f, 0f]] {
	CREATE out SET q = $q, bf = (SELECT VALUE id FROM bpts WHERE point <|1,EUCLIDEAN|> $q);
};
SELECT q, bf FROM out ORDER BY q;
{ LET $q = [5f, 0f]; RETURN SELECT VALUE id FROM bpts WHERE point <|1,EUCLIDEAN|> $q; };
-- Before the block's own LET, the statement parameter is still the one read.
{
	LET $before = (SELECT VALUE id FROM pts WHERE point <|1,40|> $q);
	LET $q = [5f, 0f];
	RETURN $before;
};
-- A later LET of the block shadows its name as the first one does.
{
	LET $a = 1;
	LET $q = [5f, 0f];
	RETURN SELECT VALUE id FROM bpts WHERE point <|1,EUCLIDEAN|> $q;
};
