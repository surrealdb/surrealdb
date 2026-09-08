/**
[test]
reason = "Repair script for index_hnsw.surql: the manual step the 3.3.0 betas require before a pre-shared-doc-ID HNSW index can be queried."
run = false
*/
REBUILD INDEX hnsw_pts ON pts;
