/**
[test]
run = false
reason = "Setup only: two tables that name their columns differently, read by set_operation_chains, set_operation_chain_mixed, set_operation_chain_intersect, set_operation_column_count and union_column_names"
*/

CREATE un_a:1 SET k = 1, name = 'ada' RETURN NONE;
CREATE un_a:2 SET k = 2, name = 'bob' RETURN NONE;
CREATE un_b:1 SET k2 = 3, label = 'cy' RETURN NONE;
CREATE un_b:2 SET k2 = 2, label = 'bob' RETURN NONE;
