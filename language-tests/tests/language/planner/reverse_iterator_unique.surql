/**
[env]

[test]
reason = "Tests reverse iteration with a unique index using ORDER BY DESC"

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ v: 15 }, { v: 14 }, { v: 13 }]"

[[test.results]]
value = "[{ v: 15 }, { v: 14 }, { v: 13 }]"

[[test.results]]
value = "[{ v: 15 }, { v: 14 }, { v: 13 }, { v: 12 }, { v: 11 }, { v: 10 }, { v: 9 }, { v: 8 }, { v: 7 }, { v: 6 }, { v: 5 }, { v: 4 }, { v: 3 }, { v: 2 }, { v: 1 }]"

*/
{
    DEFINE INDEX idx ON TABLE i COLUMNS v UNIQUE;
    FOR $i IN 1..=15 { CREATE i:[$i] SET v = $i; };
    RETURN "OK";
};
SELECT v FROM i WHERE v > 5 ORDER BY v DESC LIMIT 3;
SELECT v FROM i ORDER BY v DESC LIMIT 3;
SELECT v FROM i ORDER BY v DESC;
