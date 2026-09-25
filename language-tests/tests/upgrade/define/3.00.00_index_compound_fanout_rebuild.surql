/**
[test]
reason = "Repair script for 3.00.00_index_compound_fanout.surql: the manual step an operator runs so an index written before the writer recorded every element combination holds them all."
run = false
*/
REBUILD INDEX tagged_pair ON tagged;
