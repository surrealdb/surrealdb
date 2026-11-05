/**
[env]
imports = ["reproductions/order_by_restricted_field_limit_import.surql"]
auth = { namespace = "test", database = "test", access = "test", rid = "users:alice" }

[test]
reason = """
A field-level SELECT permission on an ORDER BY field withholds that ORDER BY
from access-path selection, so that no index walks the field in true value
order and leaks the ordering of values the actor may not read
(`order_by_field_select_perm_leak.surql` pins that property).

Withholding decides which access path may be chosen. It is not a statement
that the query needs no ordering, and LIMIT / START must still be applied to
the ordered rows: this record user owns every row, so the permission grants
the sort key on every one of them and the values are fully visible. Read as
"no ordering required", the LIMIT is pushed into the scan and the sort above
receives an already-truncated record-id-ordered prefix, which answers with the
wrong rows.

Each source route reaches the scan differently -- a literal table, a table
resolved from a call at plan time, and a B-tree access path chosen for the
WHERE clause -- and each must apply the LIMIT after the sort.
"""

# The permission grants `score` on every row, so the key is visible.
[[test.results]]
value = "[10, 40, 20, 30]"

[[test.results]]
value = "[40, 30, 20, 10]"

# Literal table source.
[[test.results]]
value = "[40, 30]"

[[test.results]]
value = "[10, 20]"

[[test.results]]
value = "[30, 20]"

# B-tree access path chosen for the WHERE clause.
[[test.results]]
value = "[40, 30]"

# Table name resolved from a call.
[[test.results]]
value = "[40, 30]"

[[test.results]]
value = "[30, 20]"

*/

SELECT VALUE score FROM doc;
SELECT VALUE score FROM doc ORDER BY score DESC;
SELECT VALUE score FROM doc ORDER BY score DESC LIMIT 2;
SELECT VALUE score FROM doc ORDER BY score ASC LIMIT 2;
SELECT VALUE score FROM doc ORDER BY score DESC START 1 LIMIT 2;
SELECT VALUE score FROM doc WHERE owner = users:alice ORDER BY score DESC LIMIT 2;
SELECT VALUE score FROM type::table(fn::tb()) ORDER BY score DESC LIMIT 2;
SELECT VALUE score FROM type::table(fn::tb()) ORDER BY score DESC START 1 LIMIT 2;
