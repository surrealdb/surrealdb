/**
[env]
imports = ["reproductions/order_by_field_select_perm_limit_import.surql"]
auth = { namespace = "test", database = "test", access = "test", rid = "users:alice" }

[test]
reason = "An ORDER BY that is withheld from access-path selection (because the sort field carries a field-level SELECT permission) must still not suppress the statement's LIMIT/START. Withholding keeps the scan in record-id order, so the LIMIT belongs above the Sort; pushing it into the scan hands the Sort an already-truncated record-id-ordered prefix and returns rows that contradict the values the actor can see. Every row here belongs to the signed-in owner, so the sort key is fully visible and the correct answer is the plain value order."

# The sort key is visible to the owner on every row.
[[test.results]]
value = "[10, 40, 20, 30]"

# ORDER BY score DESC LIMIT 2 over a table scan: the two largest visible values.
[[test.results]]
value = "[40, 30]"

# ORDER BY score ASC LIMIT 2: the two smallest.
[[test.results]]
value = "[10, 20]"

# START skips within the sorted stream, not within the scan's record-id order.
[[test.results]]
value = "[30, 20]"

# Same shape over the B-tree access path (`owner` is indexed).
[[test.results]]
value = "[40, 30]"

# Control: the DynamicScan route resolves the access path at execute time and
# must stay correct.
[[test.results]]
value = "[40, 30]"

*/

SELECT VALUE score FROM doc;
SELECT VALUE score FROM doc ORDER BY score DESC LIMIT 2;
SELECT VALUE score FROM doc ORDER BY score ASC LIMIT 2;
SELECT VALUE score FROM doc ORDER BY score DESC START 1 LIMIT 2;
SELECT VALUE score FROM doc WHERE owner = users:alice ORDER BY score DESC LIMIT 2;
SELECT VALUE score FROM type::table(fn::tb()) ORDER BY score DESC LIMIT 2;
