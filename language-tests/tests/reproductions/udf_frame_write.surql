/**
[env]
imports = ["reproductions/udf_frame_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "user:reader" }

[test]
reason = """
	A `select` PERMISSIONS guard that reaches a write through a user-defined
	function body is refused whichever way the guard is reached
	(GHSA-66r2-5gwj-gxm2). A record dereference evaluates the linked table's
	guard in the permission frame just as a scan does, so the function body
	runs under the write-refusing options and the read fails instead of
	committing the write.
"""

# 0: following the link evaluates `secret`'s guard; the write inside it is
# refused. The wording differs per engine, so only the error is asserted.
[[test.results]]
error = true

# 1: the refused write left nothing behind.
[[test.results]]
value = "[]"
*/

-- 0: the dereference reaches the guard, which reaches the write.
SELECT link.name FROM pub;

-- 1: `audit:reader` must be absent.
SELECT * FROM audit;
