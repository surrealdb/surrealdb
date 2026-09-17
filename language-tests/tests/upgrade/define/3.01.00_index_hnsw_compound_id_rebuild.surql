/**
[test]
reason = "Repair script for 3.01.00_index_hnsw_compound_id.surql: the manual step an index holding a record id with a nested number requires, because the doc-ID was recorded under a spelling no record is stored under."
run = false
*/
REBUILD INDEX hnsw_cpt ON cpt;
