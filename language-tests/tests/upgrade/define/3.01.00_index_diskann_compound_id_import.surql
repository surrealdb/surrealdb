/**
[test]
introduced = ">=3.1.0-beta.3"
reason = "Setup for 3.01.00_index_diskann_compound_id.surql: a DiskANN index built by the older binary, holding a record id that carries a number."

[[test.results]]
value = "[{ id: dpt:1, point: [3f, 0f, 0f, 0f] }]"

[[test.results]]
value = "[{ id: dpt:2, point: [4f, 0f, 0f, 0f] }]"

[[test.results]]
value = "[{ id: dpt:[5, 'e'], point: [5f, 0f, 0f, 0f] }]"

[[test.results]]
value = "NONE"
*/
CREATE dpt:1 SET point = [3.0f, 0.0f, 0.0f, 0.0f];
CREATE dpt:2 SET point = [4.0f, 0.0f, 0.0f, 0.0f];
CREATE dpt:[5, 'e'] SET point = [5.0f, 0.0f, 0.0f, 0.0f];
DEFINE INDEX dann_dpt ON dpt FIELDS point DISKANN DIMENSION 4 DIST EUCLIDEAN TYPE F32;
