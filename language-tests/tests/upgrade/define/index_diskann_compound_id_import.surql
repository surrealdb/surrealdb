/**
[test]
reason = "Setup for index_diskann_compound_id.surql: a DiskANN index built by the older binary, for the upgraded one to write a compound record id into."

[[test.results]]
value = "[{ id: dcp:1, point: [3, 0, 0, 0] }]"

[[test.results]]
value = "[{ id: dcp:2, point: [4, 0, 0, 0] }]"

[[test.results]]
value = "NONE"
*/
CREATE dcp:1 SET point = [3, 0, 0, 0];
CREATE dcp:2 SET point = [4, 0, 0, 0];
DEFINE INDEX diskann_cpt ON dcp FIELDS point DISKANN DIMENSION 4 DIST EUCLIDEAN TYPE F32;
