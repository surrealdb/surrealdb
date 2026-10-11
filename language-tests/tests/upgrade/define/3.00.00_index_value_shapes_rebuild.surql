/**
[test]
reason = "Repair script for 3.00.00_index_value_shapes_current.surql: the manual step an operator runs so an index an older binary built records the value shapes it stores, and a column that never held an array plans as one."
run = false
*/
REBUILD INDEX shape_plain_time ON shape_plain;
