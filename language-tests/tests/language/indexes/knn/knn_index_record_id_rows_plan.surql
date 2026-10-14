/**
[env]

[test]
reason = "The plans of knn_index_record_id_rows: every KNN query here is served by a KnnScan on the HNSW index, whether its rows need only their id or more of the record, and whatever COMPUTED field or field SELECT permission the table declares, `count()` included. A condition the search filters candidates with is evaluated inside the search."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: id] {rows: 3}
    KnnScan [ctx: Db] [index: hn, k: 3, ef: 40, dimension: 2, records: not read] {rows: 3}

Total rows: 3''''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: *] {rows: 1}
    Aggregate [ctx: Db] [mode: GROUP ALL] {rows: 1}
        KnnScan [ctx: Db] [index: hn, k: 3, ef: 40, dimension: 2] {rows: 1}

Total rows: 1''''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: id, name] {rows: 3}
    KnnScan [ctx: Db] [index: hn, k: 3, ef: 40, dimension: 2] {rows: 3}

Total rows: 3''''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: id] {rows: 3}
    Filter [ctx: Db] [predicate: id INSIDE [pts:1, pts:2, pts:3, pts:4]] {rows: 3}
        KnnScan [ctx: Db] [index: hn, k: 3, ef: 40, dimension: 2, predicate: id INSIDE [pts:1, pts:2, pts:3, pts:4]] {rows: 3}

Total rows: 3''''

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: id] {rows: 3}
    KnnScan [ctx: Db] [index: hn, k: 3, ef: 40, dimension: 2, records: not read] {rows: 3}

Total rows: 3''''

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: id, label] {rows: 3}
    ProjectLayer [ctx: Db] [columns: label = 'c' + <string> n : string @computed] {rows: 3}
        KnnScan [ctx: Db] [index: hn, k: 3, ef: 40, dimension: 2] {rows: 3}

Total rows: 3"'''

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: id] {rows: 3}
    KnnScan [ctx: Db] [index: hn, k: 3, ef: 40, dimension: 2, records: not read] {rows: 3}

Total rows: 3''''

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: id] {rows: 3}
    KnnScan [ctx: Db] [index: hn, k: 3, ef: 40, dimension: 2] {rows: 3}

Total rows: 3''''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: *] {rows: 1}
    Aggregate [ctx: Db] [mode: GROUP ALL] {rows: 1}
        KnnScan [ctx: Db] [index: hn, k: 3, ef: 40, dimension: 2] {rows: 1}

Total rows: 1''''
*/

DEFINE INDEX hn ON pts FIELDS point HNSW DIMENSION 2;
FOR $i IN 0..6 {
	CREATE type::record('pts', $i) SET point = [<float> $i, 0f], name = 'p' + <string> $i;
};
LET $q = [0f, 0f];
EXPLAIN ANALYZE SELECT id FROM pts WHERE point <|3,40|> $q;
EXPLAIN ANALYZE SELECT count() FROM pts WHERE point <|3,40|> $q GROUP ALL;
EXPLAIN ANALYZE SELECT id, name FROM pts WHERE point <|3,40|> $q;
-- A condition the search filters its candidates with reads each candidate's
-- record, even when the rows need only their id.
EXPLAIN ANALYZE SELECT id FROM pts WHERE id IN [pts:1, pts:2, pts:3, pts:4] AND point <|3,40|> $q;

DEFINE INDEX hn ON cmp FIELDS point HNSW DIMENSION 2;
DEFINE FIELD label ON cmp TYPE string COMPUTED 'c' + <string> n;
FOR $i IN 0..6 {
	CREATE type::record('cmp', $i) SET point = [<float> $i, 0f], n = $i;
};
EXPLAIN ANALYZE SELECT id FROM cmp WHERE point <|3,40|> $q;
EXPLAIN ANALYZE SELECT id, label FROM cmp WHERE point <|3,40|> $q;

DEFINE INDEX hn ON prm FIELDS point HNSW DIMENSION 2;
DEFINE FIELD secret ON prm PERMISSIONS FOR select NONE;
FOR $i IN 0..6 {
	CREATE type::record('prm', $i) SET point = [<float> $i, 0f], secret = $i;
};
EXPLAIN ANALYZE SELECT id FROM prm WHERE point <|3,40|> $q;

DEFINE INDEX hn ON rid FIELDS point HNSW DIMENSION 2;
DEFINE FIELD id ON rid PERMISSIONS FOR select NONE;
FOR $i IN 0..6 {
	CREATE type::record('rid', $i) SET point = [<float> $i, 0f];
};
EXPLAIN ANALYZE SELECT id FROM rid WHERE point <|3,40|> $q;
-- `count()` reads no field, but its rows are built holding `id`.
EXPLAIN ANALYZE SELECT count() FROM rid WHERE point <|3,40|> $q GROUP ALL;
