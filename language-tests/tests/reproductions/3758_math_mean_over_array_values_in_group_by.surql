/**
[env]
timeout = 5000

[test]
reason = "Pins current behaviour, by design: in a GROUP BY, math::mean aggregates one number per row of the group, so a per-row array value is skipped like any other value that is not a number, and a group with no numbers answers NaN (outside GROUP BY the same call averages each row's array). Nesting a per-row mean inside the aggregate is refused as a nested aggregate."
issue = 3758

[[test.results]]
value = "[{ id: r:1, p: 1, serie: { a: 1f, b: 3f } }]"

[[test.results]]
value = "[{ id: r:2, p: 1, serie: { a: 5f, b: 7f } }]"

[[test.results]]
value = "[{ id: r:3, p: 2, serie: { a: 2f } }]"

[[test.results]]
value = "[{ m: NaN, p: 1 }, { m: NaN, p: 2 }]"
[[test.results]]
value = "[{ m: 2f, p: 1 }, { m: 6f, p: 1 }, { m: 2f, p: 2 }]"

[[test.results]]
error = "Invalid query: Nested aggregate functions are not supported"
*/

CREATE r:1 SET p = 1, serie = { a: 1.0f, b: 3.0f };
CREATE r:2 SET p = 1, serie = { a: 5.0f, b: 7.0f };
CREATE r:3 SET p = 2, serie = { a: 2.0f };
SELECT p, math::mean(object::values(serie)) AS m FROM r GROUP BY p ORDER BY p;
SELECT p, math::mean(object::values(serie)) AS m FROM r ORDER BY p, m;
SELECT p, math::mean(math::mean(object::values(serie))) AS m FROM r GROUP BY p ORDER BY p;
