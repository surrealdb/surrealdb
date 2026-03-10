/**
[test]
reason = "Test multiple aggregates in a single expression"

[[test.results]]
value = "'OK'"

[[test.results]]
value = '[{ combined: 3f, t: 1 }, { combined: 5f, t: 2 }]'

[[test.results]]
value = '[{ avg_sum: 2, t: 1 }, { avg_sum: 3, t: 2 }]'

[[test.results]]
value = '[{ "math::sum(a) + math::sum(b)": 9, t: 1 }, { "math::sum(a) + math::sum(b)": 13, t: 2 }]'

*/

{
    CREATE m:1 SET a = 1, b = 2, t = 1;
    CREATE m:2 SET a = 2, b = 4, t = 1;
    CREATE m:3 SET a = 3, b = 3, t = 2;
    CREATE m:4 SET a = 4, b = 3, t = 2;
    RETURN "OK";
};

-- Test: SUM + MEAN (different aggregate functions)
-- For t=1: SUM(a) = 1+2 = 3, MEAN(b) = (2+4)/2 = 3, result = (3+3)/2 = 3
-- For t=2: SUM(a) = 3+4 = 7, MEAN(b) = (3+3)/2 = 3, result = (7+3)/2 = 5
SELECT (math::sum(a) + math::mean(b)) / 2 AS combined, t FROM m GROUP BY t;

-- Test: Two sums divided by 4
-- For t=1: (SUM(a) + SUM(b)) / 4 = (3 + 6) / 4 = 9/4 = 2 (integer division)
-- For t=2: (SUM(a) + SUM(b)) / 4 = (7 + 6) / 4 = 13/4 = 3 (integer division)
SELECT (math::sum(a) + math::sum(b)) / 4 AS avg_sum, t FROM m GROUP BY t;

-- Test: Direct sum of two sums (no alias)
-- For t=1: 3 + 6 = 9
-- For t=2: 7 + 6 = 13
SELECT math::sum(a) + math::sum(b), t FROM m GROUP BY t;
