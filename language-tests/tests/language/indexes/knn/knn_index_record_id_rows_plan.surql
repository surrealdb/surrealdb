/**
[env]
planner-strategy = ["all-ro", "best-effort-ro"]

[test]
reason = "The plans of knn_index_record_id_rows, pinned to the streaming strategies that render EXPLAIN. `records: not read` on the KnnScan (EXPLAIN ANALYZE) says its rows were built from the record ids the search returned: only when nothing reads more of a row than its id. A projected field, a COMPUTED field on the table, or a field SELECT permission on it — even one that does not restrict the caller — keeps the records read, and a condition the search filters candidates with reads them inside the search."

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
'Aggregate [ctx: Db] [mode: GROUP ALL] {rows: 1}
    KnnScan [ctx: Db] [index: hn, k: 3, ef: 40, dimension: 2, records: not read] {rows: 3}

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
    KnnScan [ctx: Db] [index: hn, k: 3, ef: 40, dimension: 2] {rows: 3}

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

DEFINE INDEX hn ON prm FIELDS point HNSW DIMENSION 2;
DEFINE FIELD secret ON prm PERMISSIONS FOR select NONE;
FOR $i IN 0..6 {
	CREATE type::record('prm', $i) SET point = [<float> $i, 0f], secret = $i;
};
EXPLAIN ANALYZE SELECT id FROM prm WHERE point <|3,40|> $q;
