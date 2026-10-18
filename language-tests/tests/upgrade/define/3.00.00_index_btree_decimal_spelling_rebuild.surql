/**
[test]
reason = "Repair script for 3.00.00_index_btree_decimal_spelling.surql: the manual step an operator runs so a B-tree index an older binary built spells its decimals without trailing zeros."
run = false
*/
REBUILD INDEX dec_idx_c ON dec_idx;
REBUILD INDEX dec_uniq_c ON dec_uniq;
