/**
[env]

[test]
reason = "Tests reverse iteration with range queries using ORDER BY DESC"

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ id: t:[10] }, { id: t:[9] }, { id: t:[8] }]"

[[test.results]]
value = "[{ id: t:[9] }, { id: t:[8] }, { id: t:[7] }]"

*/
{
    FOR $i IN 1..15 { CREATE t:[$i]; };
    RETURN "OK";
};

SELECT * FROM t:[5]..=[10] ORDER BY id DESC LIMIT 3;
SELECT * FROM t:[5]..[10] ORDER BY id DESC LIMIT 3;
