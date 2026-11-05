/**
[test]
run = false
reason = "Setup only: numbers with repeats, a NULL and a missing value, and two tables of numbers that cancel and of decimals, read by sum_avg_distinct, sum_avg_distinct_nested and sum_avg_distinct_two_arguments"
*/

CREATE ds_t:1 SET k = 1, v = 1, w = 2.5, grp = 'x' RETURN NONE;
CREATE ds_t:2 SET k = 2, v = 1, w = 2.5, grp = 'x' RETURN NONE;
CREATE ds_t:3 SET k = 3, v = 3, w = 4, grp = 'x' RETURN NONE;
CREATE ds_t:4 SET k = 4, grp = 'y' RETURN NONE;
CREATE ds_t:5 SET k = 5, v = 3, grp = 'y' RETURN NONE;
CREATE ds_t:6 SET k = 6, v = NULL, grp = 'z' RETURN NONE;
CREATE ds_z:1 SET v = 1 RETURN NONE;
CREATE ds_z:2 SET v = -1 RETURN NONE;
CREATE ds_d:1 SET v = 1.5dec RETURN NONE;
CREATE ds_d:2 SET v = 2.5dec RETURN NONE;
CREATE ds_d:3 SET v = 1.5dec RETURN NONE;
