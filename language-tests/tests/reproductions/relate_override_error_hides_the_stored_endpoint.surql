/**
[env]
imports = ["reproductions/relate_override_error_hides_the_stored_endpoint_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "user:alice" }

[test]
reason = """
	Re-RELATE onto an existing edge with a different endpoint is correctly
	refused, but a caller with write-but-not-read access to the edge must not
	learn its hidden `in`/`out` from the refusal (GHSA-3rcm-c653-wgmr). The stored
	endpoint is never echoed, and - the oracle this test pins - a wrong `in` and a
	wrong `out` return the same error, so which endpoint differs cannot be told
	apart and read back one query at a time. The seeded edge is
	person:carol->wrote:edge77->book:diary. The first re-RELATE moves only `in`
	(to person:mallory), the second only `out` (to book:other); on the unfixed
	code the first named `in` and the second `out`, distinguishing them. A
	permission-checked caller now gets the endpoint-agnostic refusal for both,
	and both are still rejected.
"""

[[test.results]]
error = "Cannot change the `in` or `out` endpoint of an existing edge"

[[test.results]]
error = "Cannot change the `in` or `out` endpoint of an existing edge"
*/

RELATE person:mallory->wrote:edge77->book:diary;
RELATE person:carol->wrote:edge77->book:other;
