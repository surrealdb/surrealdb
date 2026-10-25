/**
[test]
run = false
reason = "The tables the truncate_table tests remove rows from, a table they must leave alone, and a table whose rows nobody may delete"
*/

CREATE truncate_a:1 SET a = 7 RETURN NONE;
CREATE truncate_a:2 SET a = 8 RETURN NONE;
CREATE truncate_b:1 SET a = 1 RETURN NONE;

DEFINE TABLE truncate_kept SCHEMALESS PERMISSIONS FOR select, create FULL, FOR delete NONE;
CREATE truncate_kept:1 SET a = 7 RETURN NONE;
CREATE truncate_kept:2 SET a = 8 RETURN NONE;
