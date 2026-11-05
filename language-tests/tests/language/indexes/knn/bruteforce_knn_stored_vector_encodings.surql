/**
[env]
namespace = true
database = true

[test]
reason = """
A brute-force KNN ranks a record by the vector it holds, whatever encoding the
record stores the vector in. The cases are:
- floats an `f32` holds exactly;
- floats that need an `f64`;
- integers;
- a mix of integers and floats;
- decimals;
- nested arrays, flattened in order.

The path may lead into nested objects. A record whose path reaches no vector of
the query's dimension is skipped: another dimension, a non-number element, a
missing field, or an intermediate that is not an object. A literal query vector
and a parameter's value rank the same records at the same distances.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ d: 0.37416573867739417f, id: v:f64 }, { d: 1f, id: v:dec }, { d: 1f, id: v:f32 }, { d: 1.118033988749895f, id: v:mix }, { d: 2f, id: v:int }, { d: 3f, id: v:nest }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ d: 0.37416573867739417f, id: v:f64 }, { d: 1f, id: v:dec }, { d: 1f, id: v:f32 }, { d: 1.118033988749895f, id: v:mix }, { d: 2f, id: v:int }, { d: 3f, id: v:nest }]"

[[test.results]]
value = "[{ d: 0.37416573867739417f, id: v:f64 }, { d: 1f, id: v:dec }]"

[[test.results]]
value = "[{ d: 1f, id: w:a }, { d: 5f, id: w:b }]"

*/
{
	CREATE v:f32 SET emb = { v: [1f, 0f, 0f] };
	CREATE v:f64 SET emb = { v: [0.1f, 0.2f, 0.3f] };
	CREATE v:int SET emb = { v: [2, 0, 0] };
	CREATE v:mix SET emb = { v: [1, 0.5f, 0] };
	CREATE v:dec SET emb = { v: [0dec, 1dec, 0dec] };
	CREATE v:nest SET emb = { v: [[0f], [0f, 3f]] };
	CREATE v:wide SET emb = { v: [1f, 1f, 1f, 1f] };
	CREATE v:str SET emb = { v: [1f, 'x', 0f] };
	CREATE v:missing SET emb = {};
	CREATE v:flat SET emb = [1f, 2f, 3f];
	CREATE w:a SET v = [1f, 0f];
	CREATE w:b SET v = [3, 4];
	CREATE w:c SET v = NONE;
	RETURN NONE;
};
SELECT id, vector::distance::knn() AS d FROM v WHERE emb.v <|10,EUCLIDEAN|> [0f, 0f, 0f] ORDER BY d, id;
LET $q = [0f, 0f, 0f];
SELECT id, vector::distance::knn() AS d FROM v WHERE emb.v <|10,EUCLIDEAN|> $q ORDER BY d, id;
SELECT id, vector::distance::knn() AS d FROM v WHERE emb.v <|2,EUCLIDEAN|> $q ORDER BY d, id;
SELECT id, vector::distance::knn() AS d FROM w WHERE v <|5,EUCLIDEAN|> [0f, 0f] ORDER BY d, id;
