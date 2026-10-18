/**
[env]
imports = ["language/morsel/top_k_permissions_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "person:bob" }
timeout = 60000

[test]
reason = "a top-k over a SCHEMAFULL table judges the select permission of each row and of each field before it keeps a row, so a record user's top-k holds the visible rows in the order the serial run gives, whatever width the scan runs at"

[[test.results]]
value = "[{ k: 0, n: 11 }, { k: 0, n: 22 }, { k: 0, n: 44 }, { k: 0, n: 55 }, { k: 0, n: 77 }]"

[[test.results]]
value = "[{ n: 1 }, { n: 2 }, { n: 4 }, { n: 5 }, { n: 7 }]"

[[test.results]]
value = "[{ hidden: NONE, n: 43 }, { hidden: NONE, n: 65 }, { hidden: NONE, n: 76 }, { hidden: NONE, n: 98 }, { hidden: NONE, n: 109 }]"

[[test.results]]
value = "[{ n: 1496 }, { n: 1474 }, { n: 1463 }, { n: 1441 }, { n: 1430 }]"
*/

SELECT n, k FROM item ORDER BY k LIMIT 5;
SELECT n FROM item ORDER BY hidden LIMIT 5;
SELECT n, hidden FROM item ORDER BY k DESC LIMIT 5 START 2;
SELECT n FROM item ORDER BY k, n DESC LIMIT 5;
