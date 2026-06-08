/**
[test]
reason = "Every math aggregate (sum/mean/min/max/stddev/variance/median) over a GROUP BY across two groups. Streaming planner only — its stddev/variance use Welford's online algorithm and differ slightly from the legacy path."

[env]
planner-strategy = ["all-ro", "best-effort-ro"]

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ category: 'a', total: 10f }, { category: 'b', total: 35f }]"

[[test.results]]
value = "[{ avg: 3.3333333333333335f, category: 'a' }, { avg: 11.666666666666666f, category: 'b' }]"

[[test.results]]
value = "[{ category: 'a', smallest: 2 }, { category: 'b', smallest: 5 }]"

[[test.results]]
value = "[{ category: 'a', largest: 5 }, { category: 'b', largest: 20 }]"

[[test.results]]
value = "[{ category: 'a', spread: 1.5275252316519465f }, { category: 'b', spread: 7.637626158259733f }]"

[[test.results]]
value = "[{ category: 'a', variance: 2.333333333333333f }, { category: 'b', variance: 58.33333333333333f }]"

[[test.results]]
value = "[{ category: 'a', mid: 3f }, { category: 'b', mid: 10f }]"

[[test.results]]
value = "2.5f"

[[test.results]]
value = "2f"

*/

{
    CREATE samp:1 SET category = 'a', n = 2;
    CREATE samp:2 SET category = 'a', n = 3;
    CREATE samp:3 SET category = 'a', n = 5;
    CREATE samp:4 SET category = 'b', n = 5;
    CREATE samp:5 SET category = 'b', n = 10;
    CREATE samp:6 SET category = 'b', n = 20;
    RETURN NONE;
};

SELECT category, math::sum(n) AS total FROM samp GROUP BY category ORDER BY category;
SELECT category, math::mean(n) AS avg FROM samp GROUP BY category ORDER BY category;
SELECT category, math::min(n) AS smallest FROM samp GROUP BY category ORDER BY category;
SELECT category, math::max(n) AS largest FROM samp GROUP BY category ORDER BY category;
SELECT category, math::stddev(n) AS spread FROM samp GROUP BY category ORDER BY category;
SELECT category, math::variance(n) AS variance FROM samp GROUP BY category ORDER BY category;
SELECT category, math::median(n) AS mid FROM samp GROUP BY category ORDER BY category;

RETURN math::median([1, 2, 3, 4]);
RETURN math::median([1, 2, 3]);
