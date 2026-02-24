/**
[test]

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ group: 1, one: 5f, two: 5f }, { group: 2, one: 7f, two: 12f }]"

[[test.results]]
value = "[{ group: 1, one: 7f, two: 6f }, { group: 2, one: 7f, two: 13f }]"

[[test.results]]
value = "[{ group: 1, one: 7f, two: 7f }, { group: 2, one: 9f, two: 13f }]"

*/
{
    CREATE test:1 SET group = 1, one = 1.7, two = 2.4;
    CREATE test:2 SET group = 1, one = 4.7, two = 3.9;
    CREATE test:3 SET group = 2, one = 3.2, two = 9.7;
    CREATE test:4 SET group = 2, one = 4.4, two = 3.0;
    RETURN "OK";
};

SELECT group, math::sum(math::floor(one)) AS one, math::sum(math::floor(two)) AS two FROM test GROUP BY group;
SELECT group, math::sum(math::round(one)) AS one, math::sum(math::round(two)) AS two FROM test GROUP BY group;
SELECT group, math::sum(math::ceil(one)) AS one, math::sum(math::ceil(two)) AS two FROM test GROUP BY group;
