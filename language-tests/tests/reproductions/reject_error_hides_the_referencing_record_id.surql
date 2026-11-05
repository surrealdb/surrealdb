/**
[env]
imports = ["reproductions/reject_error_hides_the_referencing_record_id_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "user:alice" }

[test]
reason = """
	Deleting a record that a SELECT-restricted row references ON DELETE REJECT is
	correctly refused, but the error must not name the referencing record: a
	permission-checked caller cannot SELECT that row, so disclosing its id crosses
	the row-level SELECT boundary (GHSA-ff2q-h4x7-cpj4). The reference existing is
	the intended integrity signal; its id is withheld. On the unfixed code the
	error named the hidden `report:hidden`.

	The redaction is the caller's decision, so it holds through a cascade too and
	covers every hidden id the cascade touches: deleting the visible
	`project:visible` cascades into the hidden `note:hidden`, whose own delete the
	hidden `lock:hidden` rejects. That rejection is raised from a cascade frame
	with enforcement suppressed, but the caller is still the permission-checked
	record user, so the error names neither the referencing `lock:hidden` nor the
	cascade-reached `note:hidden` the caller could not SELECT; only the visible
	record the caller deleted itself is named. On the unfixed code the cascade
	frame looked unchecked and named both hidden records.

	The direct delete still names `asset:secret`: it is the caller's own target,
	which the caller named and may read, so it is not a hidden id.
"""

[[test.results]]
error = "Cannot delete `asset:secret` as it is referenced by another record with an ON DELETE REJECT clause"

[[test.results]]
error = "An error occurred while updating references for `project:visible`: Cannot delete the record as it is referenced by another record with an ON DELETE REJECT clause"
*/

DELETE asset:secret;

DELETE project:visible;
