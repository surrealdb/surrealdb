/**
[test]

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ g: 1, v: [1, 2, 3] }, { g: 2, v: [4, 5, 6] }]"

*/


{
    CREATE tgt:1 SET v = 1, g = 1;
    CREATE tgt:2 SET v = 2, g = 1;
    CREATE tgt:3 SET v = 3, g = 1;
    CREATE tgt:4 SET v = 4, g = 2;
    CREATE tgt:5 SET v = 5, g = 2;
    CREATE tgt:6 SET v = 6, g = 2;
    RETURN "OK";
};

SELECT v, g FROM tgt GROUP BY g;
