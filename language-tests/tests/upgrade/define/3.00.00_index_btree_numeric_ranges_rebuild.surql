/**
[test]
reason = "Repair script for 3.00.00_index_btree_numeric_ranges_current.surql: the step an operator runs so an index an older binary built records the value shapes it stores, and its numeric column, which never held an array, plans its ranges, order and counts from the index again."
run = false
*/
REBUILD INDEX num_n ON num;
REBUILD INDEX numu_n ON numu;
