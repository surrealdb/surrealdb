/**
[test]
reason = "Repair script for 3.00.00_index_btree_compound_ranges_current.surql: the step an operator runs so an index an older binary built records the value shapes it stores, and its columns, which never held an array, plan their ranges from the index again."
run = false
*/
REBUILD INDEX cmp_ab ON cmp;
REBUILD INDEX cmpu_ab ON cmpu;
