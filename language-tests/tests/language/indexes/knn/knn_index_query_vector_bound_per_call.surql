/**
[test]
wip = true
reason = """
An approximate KNN (`<|k,EF|>`) over an ANN index whose query vector a closure
argument or a `FOR` variable binds ranks by that binding's value, as a brute-force
KNN does: `pts:5` for `[5f, 0f]` and `pts:3` for `[3f, 0f]`, never the statement
parameter of the same name. The engine finds no vector to search the index with
when the statement is planned, and answers no row.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[[pts:5], [pts:3]]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ hn: [pts:3], q: [3f, 0f] }, { hn: [pts:5], q: [5f, 0f] }]"
*/

DEFINE INDEX hn ON pts FIELDS point HNSW DIMENSION 2;
FOR $i IN 0..6 {
	CREATE type::record('pts', $i) SET point = [<float> $i, 0f];
};
RETURN [[5f, 0f], [3f, 0f]].map(|$q| SELECT VALUE id FROM pts WHERE point <|1,40|> $q);
FOR $q IN [[5f, 0f], [3f, 0f]] {
	CREATE out SET q = $q, hn = (SELECT VALUE id FROM pts WHERE point <|1,40|> $q);
};
SELECT q, hn FROM out ORDER BY q;
