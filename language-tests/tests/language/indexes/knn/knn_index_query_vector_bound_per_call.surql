/**
[test]
reason = """
An approximate KNN (`<|k,EF|>`) over an ANN index whose query vector a closure
argument or a `FOR` variable binds ranks by that binding's value, as a
brute-force KNN does: `pts:5` for `[5f, 0f]` and `pts:3` for `[3f, 0f]`, never
the statement parameter of the same name. The statement is planned before the
binding has a value, so the search evaluates the vector when it runs, once per
call.
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

[[test.results]]
value = "NONE"

[[test.results]]
value = "[[pts:5], [pts:3]]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ hn: [pts:3], q: [3f, 0f] }, { hn: [pts:5], q: [5f, 0f] }]"

[[test.results]]
value = "[pts:1]"
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
-- with a statement parameter of the same name
LET $q = [1f, 0f];
RETURN [[5f, 0f], [3f, 0f]].map(|$q| SELECT VALUE id FROM pts WHERE point <|1,40|> $q);
FOR $q IN [[5f, 0f], [3f, 0f]] {
	CREATE out2 SET q = $q, hn = (SELECT VALUE id FROM pts WHERE point <|1,40|> $q);
};
SELECT q, hn FROM out2 ORDER BY q;
SELECT VALUE id FROM pts WHERE point <|1,40|> $q;
