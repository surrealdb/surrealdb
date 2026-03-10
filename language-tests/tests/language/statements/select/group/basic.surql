/**
[test]

[[test.results]]
value = "'OK'"

[[test.results]]
value = '[{ "math::mean": 2.5f, t: 1 }, { "math::mean": 3.5f, t: 2 }]'

[[test.results]]
value = '[{ "math::mean(v) + 1": 2.5f, t: 1 }, { "math::mean(v) + 1": 3.5f, t: 2 }]'

*/

{
    CREATE a:1 SET v = 1, t = 1;
    CREATE a:2 SET v = 2, t = 1;
    CREATE a:3 SET v = 2, t = 2;
    CREATE a:4 SET v = 3, t = 2;
    RETURN "OK";
};

SELECT math::mean(v + 1), t FROM a GROUP t;
SELECT math::mean(v) + 1, t FROM a GROUP t;
