/**
[test]
run = false
reason = "Setup only: one table of two rows and one of three, read by scalar_subquery_one_row, scalar_subquery_quantified_group_by and scalar_subquery_several_columns"
*/

CREATE sr_a:1 SET k = 1, name = 'ada' RETURN NONE;
CREATE sr_a:2 SET k = 2, name = 'bob' RETURN NONE;
CREATE sr_b:1 SET k2 = 2, label = 'bob' RETURN NONE;
CREATE sr_b:2 SET k2 = 2, label = 'bob again' RETURN NONE;
CREATE sr_b:3 SET k2 = 5, label = 'eve' RETURN NONE;
