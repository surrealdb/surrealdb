/**
[test]
run = false
reason = "Setup only: two tables with a column in common and a column with NULLs, read by select_output_keys, select_output_keys_aggregate_clauses, select_output_keys_set_operations and select_output_keys_nested"
*/

CREATE okey_a:1 SET k = 1, v = 'x', n = 10 RETURN NONE;
CREATE okey_a:2 SET k = 2, v = NULL, n = 20 RETURN NONE;
CREATE okey_a:3 SET k = 3, v = 'z', n = NULL RETURN NONE;
CREATE okey_b:1 SET k = 1, w = 100 RETURN NONE;
CREATE okey_b:2 SET k = 1, w = 200 RETURN NONE;
CREATE okey_b:3 SET k = 2, w = 300 RETURN NONE;
