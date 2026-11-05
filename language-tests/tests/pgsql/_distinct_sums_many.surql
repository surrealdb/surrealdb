/**
[test]
run = false
reason = "Setup only: nine thousand rows holding three thousand distinct numbers, each three times, in two groups, read by sum_avg_distinct_many"
*/

CREATE |dsm_t:0..9000| RETURN NONE;
UPDATE dsm_t SET v = record::id(id) % 3000, grp = record::id(id) % 2 RETURN NONE;
