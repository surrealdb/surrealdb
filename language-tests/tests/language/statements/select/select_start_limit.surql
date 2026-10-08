/**
[env]

[test]

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ b: true, id: i:2 }, { b: true, id: i:3 }, { b: true, id: i:4 }]"

[[test.results]]
value = "[{ b: true, id: i:2 }, { b: true, id: i:3 }, { b: true, id: i:4 }]"

*/
{
    CREATE i:0 SET b=true;
    CREATE i:1 SET b=true;
    CREATE i:2 SET b=true;
    CREATE i:3 SET b=true;
    CREATE i:4 SET b=true;
    CREATE i:5 SET b=true;
    RETURN "OK";
};
SELECT * FROM i LIMIT 3 START 2;
SELECT * FROM i WHERE b = true LIMIT 3 START 2;
