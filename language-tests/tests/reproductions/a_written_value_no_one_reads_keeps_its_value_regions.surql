/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "An IF, a block or a FOR in value position that writes still runs when nothing reads its value, and the plan that drops the unread binding still verifies"

[[test.results]]
value = "5"

[[test.results]]
value = "5"

[[test.results]]
value = "5"

[[test.results]]
value = "[t:1, t:2, t:3]"
*/
{ LET $x = IF true { CREATE t:1 RETURN NONE; 1 } ELSE { 2 }; RETURN 5 };
{ LET $x = { CREATE t:2 RETURN NONE; 3 }; RETURN 5 };
{ LET $x = FOR $i IN [3] { CREATE t:3 RETURN NONE; }; RETURN 5 };
SELECT VALUE id FROM t ORDER BY id;
