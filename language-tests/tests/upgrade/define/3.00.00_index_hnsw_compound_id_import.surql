/**
[test]
reason = "Setup for 3.00.00_index_hnsw_compound_id.surql: an HNSW index built by the older binary, for the upgraded one to write a compound record id into."

[[test.results]]
value = "[{ id: cpt:1, point: [3, 0, 0, 0] }]"

[[test.results]]
value = "[{ id: cpt:2, point: [4, 0, 0, 0] }]"

[[test.results]]
value = "NONE"
*/
CREATE cpt:1 SET point = [3, 0, 0, 0];
CREATE cpt:2 SET point = [4, 0, 0, 0];
DEFINE INDEX hnsw_cpt ON cpt FIELDS point HNSW DIMENSION 4 DIST EUCLIDEAN TYPE F32 EFC 500 M 12;
