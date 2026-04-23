/**
[test]
reason = "`id` of a simultaneously-created record should be resolvable through the record link in a SELECT permission clause (see issue #7221)."
issue = 7221

[[test.results]]
value = "{ id: a:`0123456789abcdef0124` }"

[[test.results]]
value = "[{ id: a:`0123456789abcdef0124` }]"

[[test.results]]
value = "{ id: a:0123456789abcdef0123 }"
skip-record-id-key = true

[[test.results]]
value = "[{ id: a:0123456789abcdef0123 }]"
skip-record-id-key = true

[env]
clean = true
imports = ["reproductions/7221_create_only_select_permission_id_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "user:test" }

*/

-- Baseline (separate statements): CREATE then SELECT. Both must succeed.
CREATE ONLY a:`0123456789abcdef0124`;
SELECT * FROM a:`0123456789abcdef0124`;

-- Inline form: SELECT directly from the CREATE ONLY subquery.
SELECT * FROM ONLY (CREATE ONLY a);

-- Block form: assign first, then SELECT from the variable.
{
	LET $created = (CREATE ONLY a);
	RETURN SELECT * FROM $created;
};
