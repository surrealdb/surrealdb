/**
[test]
reason = "GROUP BY + ORDER BY + TEMPFILES produces sums and a stable ordering for each group. Sister test: order_by_tempfiles_new_executor.surql covers the non-grouped path."

[env]
planner-strategy = ["all-ro", "best-effort-ro"]

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ category: 'a', total: 6 }, { category: 'b', total: 60 }, { category: 'c', total: 7 }]"

*/

{
    CREATE ext:01 SET category = 'b', n = 10;
    CREATE ext:02 SET category = 'a', n = 1;
    CREATE ext:03 SET category = 'c', n = 7;
    CREATE ext:04 SET category = 'b', n = 20;
    CREATE ext:05 SET category = 'a', n = 2;
    CREATE ext:06 SET category = 'b', n = 30;
    CREATE ext:07 SET category = 'a', n = 3;
    RETURN NONE;
};

SELECT category, math::sum(n) AS total FROM ext GROUP BY category ORDER BY category ASC TEMPFILES;
