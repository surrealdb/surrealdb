/**
[env]
namespace = true
database = true
imports = ["./a_guard_cuts_what_the_row_holds_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "user:probe" }

[test]
reason = """
	A field's declared kind is enforced on write alone, so a row written before
	the declaration — or before it changed — holds what it held. A guard that
	took the declaration at its word would lower `moved.secret` as a plain
	field cut, because `object` does not distribute, and the cut would pass the
	stored array through untouched: every `secret` in it reaches a
	permission-checked caller. The guard must cut what the row holds, so both
	fields redact alike.
"""

[[test.results]]
value = "[{ id: t:1, kept: [{ ok: 1 }, { ok: 2 }], moved: [{ ok: 1 }, { ok: 2 }] }]"
*/

SELECT * FROM t;
