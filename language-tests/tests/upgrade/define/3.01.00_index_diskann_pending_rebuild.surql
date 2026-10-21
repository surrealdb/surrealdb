/**
[test]
reason = "Repair script for 3.01.00_index_diskann_pending.surql: the manual step the 3.3.0 betas require before a pre-shared-doc-ID DiskANN index can be queried."
run = false
*/
REBUILD INDEX dann_pend ON pend;
