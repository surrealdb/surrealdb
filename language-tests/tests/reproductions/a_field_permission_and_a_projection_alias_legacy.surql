/**
[env]
namespace = true
database = true
imports = ["reproductions/a_field_permission_and_a_projection_alias_import.surql"]
auth = { namespace = "test", database = "test", access = "account", rid = "user:alice" }
planner-strategy = ["compute-only"]

[test]
reason = """
	alice owns `p:1` and `p:3`; `p:2` is bob's, so its `g` must never be
	visible. It is not, on either engine and in every shape below: the value is
	redacted at the record, before any projection can move or rename it.

	Two shapes still differ.

	A denied field read under its own name: the per-row engine leaves the key
	out of the row, the streaming engine emits it holding `NONE`. Neither
	discloses the value; they disagree on whether the row admits the field
	exists.

	A projection alias reusing the guarded name — `SELECT n AS g` — is redacted
	by the per-row engine and returned by the streaming one. `n` is unguarded
	and alice may read it; the per-row engine cuts it anyway, because
	`apply_select_field_permissions` matches field names against the projected
	output's keys, so a value that merely lands under the name `g` is treated as
	the field `g`. The streaming engine judges the record, so the alias is just
	a name.

	The streaming answer is the right one in both: a permission belongs to the
	field it is defined on, not to whatever the output happens to call a value.
	The per-row engine is not being changed to match. Its post-projection pass
	is deliberately name-keyed and idempotent over a record its `reduce` step
	has already redacted, it is the site of two past security fixes, and it
	errs closed — it hides data the caller may see rather than revealing data
	they may not. Rewriting it belongs with a security review of that pass, not
	with this parity work.
"""

[[test.results]]
value = "[{ g: 30 }, {  }, { g: 10 }]"

[[test.results]]
value = "[30, NONE, 10]"

[[test.results]]
value = "[{ g: 30, id: p:1, n: 1, owner: user:alice }, { id: p:2, n: 2, owner: user:bob }, { g: 10, id: p:3, n: 3, owner: user:alice }]"

[[test.results]]
value = "[{ n: 30 }, { n: NONE }, { n: 10 }]"

[[test.results]]
value = "[{ zz: 30 }, { zz: NONE }, { zz: 10 }]"

[[test.results]]
value = "[{ n: { inner: 30 } }, { n: { inner: NONE } }, { n: { inner: 10 } }]"

[[test.results]]
value = "[{ n: [30] }, { n: [NONE] }, { n: [10] }]"

[[test.results]]
value = "[{ n: 30 }, { n: NONE }, { n: 10 }]"

[[test.results]]
value = "[{ g: 1 }, {  }, { g: 3 }]"

[[test.results]]
value = "[{ g: 1, owner: user:alice }, { owner: user:bob }, { g: 3, owner: user:alice }]"
*/

-- The guarded field, read as itself.
SELECT g FROM p;
SELECT VALUE g FROM p;
SELECT * FROM p;

-- The guarded field renamed: the value is judged at the record, so the
-- rename carries a redaction, never a permission.
SELECT g AS n FROM p;
SELECT g AS zz FROM p;
SELECT { inner: g } AS n FROM p;
SELECT [g] AS n FROM p;
SELECT VALUE { n: g } FROM p;

-- An unguarded field renamed onto the guarded fields name.
SELECT n AS g FROM p;
SELECT owner, n AS g FROM p;

