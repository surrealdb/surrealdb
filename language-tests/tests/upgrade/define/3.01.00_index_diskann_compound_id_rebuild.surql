/**
[test]
reason = "Repair script for 3.01.00_index_diskann_compound_id.surql: the manual step a DiskANN index holding a record id with a nested number requires."
run = false
*/
REBUILD INDEX dann_dpt ON dpt;
