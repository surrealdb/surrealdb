/**
[env]
imports = ["upgrade/define/3.00.00_closure_capture_in_stored_clauses_import.surql"]

[test]
upgrade = true
introduced = "3.0.0"
reason = "Field and event clauses stored by an old binary whose closures read a database param, `$value` and `$after` still evaluate those names where the clause runs."

[[test.results]]
value = "[{ f: [10, 15, 20], g: [5, 10], id: w:1, lim: 8 }]"

[[test.results]]
value = "[[[10, 15, 20], [5, 10]]]"

[[test.results]]
value = "[[10]]"

*/
UPSERT w:1 SET lim = 8, g = 5;
SELECT VALUE [f, g] FROM w:1;
SELECT VALUE v FROM log:1;
