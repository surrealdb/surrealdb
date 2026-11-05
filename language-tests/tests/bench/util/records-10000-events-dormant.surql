/**
# `records-10000` with four events no `UPDATE` ever fires, and an empty `audit` table.
#
# Two of them fire for `CREATE` or for `DELETE` only, one for both, and one never; an
# `UPDATE` of any record evaluates each of their `WHEN` clauses to a falsy literal, which a
# write that decides the clauses ahead of time does not evaluate. A `DELETE` fires the events
# that name it. `run = false`.

[env]
imports = ["./records-10000.surql"]

[test]
run = false

[[test.results]]
value = "'OK'"
*/

{
	DEFINE TABLE audit SCHEMALESS;
	DEFINE EVENT on_create ON record WHEN $event = "CREATE" THEN (CREATE audit SET at = $after.id);
	DEFINE EVENT on_delete ON record WHEN $event = "DELETE" THEN (CREATE audit SET at = $before.id);
	DEFINE EVENT on_create_or_delete ON record WHEN $event IN ["CREATE", "DELETE"] THEN (CREATE audit SET at = $value.id);
	DEFINE EVENT never ON record WHEN false THEN (CREATE audit SET at = $value.id);

	RETURN 'OK';
}
