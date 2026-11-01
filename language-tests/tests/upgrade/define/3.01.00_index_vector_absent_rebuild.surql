/**
[test]
reason = "Repair script for 3.01.00_index_vector_absent_current.surql: the manual step an operator runs so a vector index an older binary built records whether it holds every record, and ORDER BY vector::distance::* LIMIT k over it plans as a KNN scan."
run = false
*/
REBUILD INDEX vabs_full_h_ix ON vabs_full_h;
REBUILD INDEX vabs_full_d_ix ON vabs_full_d;
