/**
[env]
imports = ["reproductions/relate_override_error_hides_the_stored_endpoint_import.surql"]

[test]
reason = """
	The companion to relate_override_error_hides_the_stored_endpoint. A caller
	whose reads are not permission-checked - here an owner - hides no endpoint, so
	the re-RELATE override error names the one that differs (GHSA-3rcm-c653-wgmr):
	an `in` mismatch and an `out` mismatch stay distinct, unlike for a
	permission-checked caller, who gets one endpoint-agnostic error for both. The
	seeded edge is person:carol->wrote:edge77->book:diary; the first re-RELATE
	moves only `in`, the second only `out`, and both are still rejected.
"""

[[test.results]]
error = "Cannot change the `in` endpoint of an existing edge"

[[test.results]]
error = "Cannot change the `out` endpoint of an existing edge"
*/

RELATE person:mallory->wrote:edge77->book:diary;
RELATE person:carol->wrote:edge77->book:other;
