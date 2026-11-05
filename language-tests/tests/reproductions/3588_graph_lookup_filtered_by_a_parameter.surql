/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
A graph lookup narrowed to the record a parameter holds, written as a
subquery over the target table or as a filter on the edge.
"""
issue = 3588

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[[account:1, account:2]]"

[[test.results]]
value = "[[account:1]]"

[[test.results]]
value = "[[account:1]]"
*/
CREATE t:1, account:1, account:2 RETURN NONE;
RELATE account:1->like->t:1 RETURN NONE;
RELATE account:2->like->t:1 RETURN NONE;
LET $accountId = account:1;
SELECT VALUE array::sort(<-like<-account) FROM t;
SELECT VALUE <-like<-(account WHERE id = $accountId) FROM t;
SELECT VALUE <-like[WHERE in = $accountId]<-account FROM t;
