/**
[test]
reason = "Repair script for 3.00.00_index_btree_identity_entry_ids.surql: the manual step an operator runs so a B-tree index an older binary built gives each record its own entry."
run = false
*/
REBUILD INDEX ent_body ON ent;
