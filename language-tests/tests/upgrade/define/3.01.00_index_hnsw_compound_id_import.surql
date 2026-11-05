/**
[test]
introduced = "3.1.0"
reason = """
Setup for 3.01.00_index_hnsw_compound_id.surql.

`cpt:[5, 'e']` is the record the older binary indexes under an id holding a
number, which is the state the test exists to read back. The plain ids beside
it are the control: they share the index and are never affected.
"""

[[test.results]]
value = "[{ id: cpt:1, point: [3, 0, 0, 0] }]"

[[test.results]]
value = "[{ id: cpt:2, point: [4, 0, 0, 0] }]"

[[test.results]]
value = "[{ id: cpt:[5, 'e'], point: [5, 0, 0, 0] }]"

[[test.results]]
value = "NONE"
*/
CREATE cpt:1 SET point = [3, 0, 0, 0];
CREATE cpt:2 SET point = [4, 0, 0, 0];
CREATE cpt:[5, 'e'] SET point = [5, 0, 0, 0];
DEFINE INDEX hnsw_cpt ON cpt FIELDS point HNSW DIMENSION 4 DIST EUCLIDEAN TYPE F32 EFC 500 M 12;
