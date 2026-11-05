/**
[test]
requires = ["diskann"]
introduced = ">=3.1.0-beta.3"
reason = "Setup for 3.01.00_index_diskann_pending.surql: records the older binary writes into a DiskANN index after defining it, which it leaves pending until a compaction folds them into the graph."

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: pend:1, point: [1f, 2f, 3f, 4f] }]"

[[test.results]]
value = "[{ id: pend:2, point: [10f, 20f, 30f, 40f] }]"

[[test.results]]
value = "[{ id: pend:3, point: [100f, 100f, 100f, 100f] }]"
*/
DEFINE INDEX dann_pend ON pend FIELDS point DISKANN DIMENSION 4 DIST EUCLIDEAN TYPE F32;
CREATE pend:1 SET point = [1.0, 2.0, 3.0, 4.0];
CREATE pend:2 SET point = [10.0, 20.0, 30.0, 40.0];
CREATE pend:3 SET point = [100.0, 100.0, 100.0, 100.0];
