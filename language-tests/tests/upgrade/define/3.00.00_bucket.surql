/**
[env]
imports = ["upgrade/define/3.00.00_bucket_import.surql"]

[env.capabilities]
allow-experimental = ["files"]

[test]
upgrade = true
reason = """
DEFINE BUCKET had no upgrade coverage at all: five other tests pin
`buckets: {  }` as an empty map and that was the whole of it, leaving
StoredBucketDefinition and BucketId untested across every release.

The new binary must decode all three stored definitions — plain, READONLY, and
one carrying a permissions clause — and must be able to write into a bucket an
older binary defined.

Object bytes are deliberately not asserted. A `memory` backend is process-local,
so files written by the old binary are gone by design once it exits; proving
object durability needs a filesystem-backed bucket rooted in the run's own
directory, which the harness cannot yet provision."""

[[test.results]]
match = '''
    $result.buckets.bkt == "DEFINE BUCKET bkt BACKEND 'memory' PERMISSIONS FULL"
        && string::contains($result.buckets.bkt_ro, "READONLY")
        && string::contains($result.buckets.bkt_perm, "PERMISSIONS WHERE $action = 'get'")
'''

[[test.results]]
value = "NONE"

[[test.results]]
value = "'written after the upgrade'"

[[test.results]]
error = true

*/
INFO FOR DB;
-- The new binary writes into a bucket an older binary defined, and reads it
-- back through the definition that binary stored.
f"bkt:/after.txt".put("written after the upgrade");
<string> f"bkt:/after.txt".get();
-- READONLY is not merely rendered back in INFO; it is still enforced.
f"bkt_ro:/nope.txt".put("should be refused");
