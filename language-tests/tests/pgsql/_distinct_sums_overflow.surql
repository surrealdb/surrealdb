/**
[test]
run = false
reason = "Setup only: integers whose sum leaves or only reaches the edge of a 64-bit integer, read by sum_avg_distinct_overflow"
*/

CREATE dso_max:1 SET v = 9223372036854775807 RETURN NONE;
CREATE dso_max:2 SET v = 9223372036854775806 RETURN NONE;
CREATE dso_max:3 SET v = 1 RETURN NONE;
CREATE dso_min:1 SET v = -9223372036854775808 RETURN NONE;
CREATE dso_min:2 SET v = -1 RETURN NONE;
CREATE dso_edge:1 SET v = 9223372036854775807 RETURN NONE;
CREATE dso_edge:2 SET v = -9223372036854775808 RETURN NONE;
CREATE dso_edge:3 SET v = 0 RETURN NONE;
