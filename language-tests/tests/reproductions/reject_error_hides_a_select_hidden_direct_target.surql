/**
[env]
imports = ["reproductions/reject_error_hides_a_select_hidden_direct_target_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "user:alice" }

[test]
reason = """
	A record user with FOR delete FULL and FOR select NONE on `vault` deletes it
	by a bare table target, which reaches `vault:1` through the delete permission
	even though the user cannot SELECT the record. `holder:1` references it ON
	DELETE REJECT, so the delete is correctly refused; the error must not name
	`vault:1`, a row the user cannot read. On the unfixed code the direct target
	was treated as caller-known and its id was rendered.
"""

[[test.results]]
error = "Cannot delete the record as it is referenced by another record with an ON DELETE REJECT clause"
*/

DELETE vault;
