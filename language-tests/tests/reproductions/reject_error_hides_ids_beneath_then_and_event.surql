/**
[env]
imports = ["reproductions/reject_error_hides_ids_beneath_then_and_event_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "user:alice" }

[test]
reason = """
	An ON DELETE THEN clause and a DEFINE EVENT body run with enforcement
	suppressed, and each here runs a nested DELETE that an ON DELETE REJECT
	refuses. The refusal still reaches the permission-checked record user, so
	whether an id is shown is decided by that principal, not by the suppressed
	frame: neither the hidden referencing `lock` record nor the hidden `item`
	the nested delete reached may be named. Only `asset:a`, the visible record
	the user deleted, is named.
"""

[[test.results]]
error = "An error occurred while updating references for `asset:a`: Cannot delete the record as it is referenced by another record with an ON DELETE REJECT clause"

[[test.results]]
error = "Error while processing event cleanup: Cannot delete the record as it is referenced by another record with an ON DELETE REJECT clause"
*/

DELETE asset:a;

DELETE widget:w;
