/**
[test]
reason = "SELECT FROM <literal record id> with LIMIT / START / both still returns the single matching row."

[env]
planner-strategy = ["all-ro", "best-effort-ro"]

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: rid_fast:a, v: 1 }]"

[[test.results]]
value = "[{ id: rid_fast:b, v: 2 }]"

[[test.results]]
value = "[{ id: rid_fast:c, v: 3 }]"

*/

{
    CREATE rid_fast:a SET v = 1;
    CREATE rid_fast:b SET v = 2;
    CREATE rid_fast:c SET v = 3;
    RETURN NONE;
};

SELECT * FROM rid_fast:a LIMIT 1;
SELECT * FROM rid_fast:b START 0;
SELECT * FROM rid_fast:c LIMIT 1 START 0;
