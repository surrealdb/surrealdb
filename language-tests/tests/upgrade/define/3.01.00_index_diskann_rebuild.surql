/**
[test]
reason = "Repair script for index_diskann.surql: the manual step the 3.3.0 betas require before a pre-shared-doc-ID DiskANN index can be queried."
run = false
*/
REBUILD INDEX dann_pts ON pts;
