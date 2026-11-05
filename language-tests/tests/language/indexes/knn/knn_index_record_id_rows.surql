/**
[test]
reason = "An index-backed KNN whose rows are read for nothing but their id returns them without reading the records (the plan is pinned in knn_index_record_id_rows_plan). Each query here returns what reading the records returns: bare ids, their distances, a count, a path dereferencing the id, a projected field, a residual condition, a function reading a field named at runtime (`type::field`), a record deleted earlier in the same transaction, a table with a COMPUTED field, and a brute-force KNN over a COMPUTED vector."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: pts:0 }, { id: pts:1 }, { id: pts:2 }]"

[[test.results]]
value = "[pts:0, pts:1, pts:2]"

[[test.results]]
value = "[{ d: 0f, id: pts:0 }, { d: 1f, id: pts:1 }, { d: 2f, id: pts:2 }]"

[[test.results]]
value = "[{ count: 3 }]"

[[test.results]]
value = "['p0', 'p1', 'p2']"

[[test.results]]
value = "[{ id: pts:0, name: 'p0' }, { id: pts:1, name: 'p1' }, { id: pts:2, name: 'p2' }]"

[[test.results]]
value = "[{ id: pts:1 }, { id: pts:2 }, { id: pts:3 }]"

[[test.results]]
value = "[{ name: 'p0' }, { name: 'p1' }, { name: 'p2' }]"


[[test.results]]
value = "[{ id: pts:1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: pts:1 }, { id: pts:2 }, { id: pts:3 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: cmp:0 }, { id: cmp:1 }, { id: cmp:2 }]"

[[test.results]]
value = "[{ id: cmp:0, label: 'c0' }, { id: cmp:1, label: 'c1' }, { id: cmp:2, label: 'c2' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: bf:0 }, { id: bf:1 }, { id: bf:2 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: bf:0 }, { id: bf:1 }, { id: bf:2 }, { id: bf:3 }, { id: bf:4 }, { id: bf:5 }, { id: bf2:0 }, { id: bf2:1 }, { id: bf2:2 }, { id: bf2:3 }, { id: bf2:4 }, { id: bf2:5 }]"

*/

DEFINE INDEX hn ON pts FIELDS point HNSW DIMENSION 2;
FOR $i IN 0..6 {
	CREATE type::record('pts', $i) SET point = [<float> $i, 0f], name = 'p' + <string> $i, n = $i;
};
LET $q = [0f, 0f];

SELECT id FROM pts WHERE point <|3,40|> $q;
SELECT VALUE id FROM pts WHERE point <|3,40|> $q;
SELECT id, vector::distance::knn() AS d FROM pts WHERE point <|3,40|> $q;
SELECT count() FROM pts WHERE point <|3,40|> $q GROUP ALL;
-- A path through the id dereferences the record itself.
SELECT VALUE id.name FROM pts WHERE point <|3,40|> $q;
SELECT id, name FROM pts WHERE point <|3,40|> $q;
SELECT id FROM pts WHERE point <|3,40|> $q AND n > 0;
-- A function reading a field named at runtime reads the records.
SELECT type::field('name') FROM pts WHERE point <|3,40|> $q;
SELECT id FROM pts WHERE point <|3,40|> $q AND type::field('name') = 'p1';

-- A record deleted earlier in the transaction is not returned.
BEGIN;
DELETE pts:0;
SELECT id FROM pts WHERE point <|3,40|> $q;
COMMIT;

DEFINE INDEX hn ON cmp FIELDS point HNSW DIMENSION 2;
DEFINE FIELD label ON cmp TYPE string COMPUTED 'c' + <string> n;
FOR $i IN 0..6 {
	CREATE type::record('cmp', $i) SET point = [<float> $i, 0f], n = $i;
};
SELECT id FROM cmp WHERE point <|3,40|> $q;
SELECT id, label FROM cmp WHERE point <|3,40|> $q;

-- A brute-force KNN scores each row's vector, a COMPUTED one included.
DEFINE FIELD cv ON bf COMPUTED [<float> x, 0f];
FOR $i IN 0..6 {
	CREATE type::record('bf', $i) SET x = $i;
};
SELECT id FROM bf WHERE cv <|3,EUCLIDEAN|> $q;
-- Over two tables too. A record with no vector is not a neighbour, so every
-- record is returned only if each one's vector was derived. (k covers both
-- tables: how k splits across sources is not what this checks.)
DEFINE FIELD cv ON bf2 COMPUTED [<float> x, 0f];
FOR $i IN 0..6 {
	CREATE type::record('bf2', $i) SET x = $i + 10;
};
SELECT id FROM bf, bf2 WHERE cv <|20,EUCLIDEAN|> $q ORDER BY id;
