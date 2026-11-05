/**
[env]
imports = ["reproductions/reject_error_hides_intermediate_cascade_records_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "user:alice" }

[test]
reason = """
	Deleting the visible `alpha:1` cascades through the hidden `beta:1` into the
	hidden `gamma:1`, whose delete the hidden `delta:1` rejects. The rejection is
	wrapped once per cascade hop, and each wrap names the record whose references
	failed; for a permission-checked caller those intermediate records are hidden,
	so neither `beta:1` nor `gamma:1` (nor the referencing `delta:1`) may appear.
	Only the visible `alpha:1` the caller named is shown. On the unfixed code each
	wrap rendered the hidden intermediate id.
"""

[[test.results]]
error = "An error occurred while updating references for `alpha:1`: An error occurred while updating references: Cannot delete the record as it is referenced by another record with an ON DELETE REJECT clause"
*/

DELETE alpha:1;
