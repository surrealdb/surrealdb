/**
[env]
namespace = true
database = true

[env.capabilities]
allow-experimental = ["files"]

[test]
reason = """
Issue 7569: bucket operations on a freshly defined bucket resolve and run. This is a smoke test only:
`DEFINE BUCKET` pre-populates the connection cache, so it does not reach the cold-cache path. The
shard-lock deadlock is pinned by `a_slow_connect_does_not_block_other_callers` in
`core/src/buc/manager.rs`, which parks a connect on an empty cache."""
issue = 7569

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "true"

[[test.results]]
value = "true"
*/

DEFINE BUCKET b BACKEND "memory";
file::put(f"b:/a.txt", "one");
file::put(f"b:/b.txt", "two");
f"b:/a.txt".exists();
f"b:/b.txt".exists();
