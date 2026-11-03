/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[env.config]
global_bucket = "memory"

[test]
reason = "A `global://` backend serves the configured global bucket at the path it names. A path that would leave itself is refused, and so is a DEFINE BUCKET with no BACKEND outside an import. Sharing between buckets over one global bucket needs a durable global bucket and is covered by the unit tests in `buc::manager`."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "'hello'"

[[test.results]]
error = "Bucket url could not be processed: Invalid global bucket path `app/../other`"

[[test.results]]
error = 'DEFINE BUCKET needs a BACKEND, such as BACKEND "global://" for the global bucket'

[[test.results]]
value = """{ sub: "DEFINE BUCKET sub BACKEND 'global://app/sub' PERMISSIONS FULL", whole: "DEFINE BUCKET whole BACKEND 'global://' PERMISSIONS FULL" }"""
*/

DEFINE BUCKET whole BACKEND "global://";
DEFINE BUCKET sub BACKEND "global://app/sub";
f"sub:/a.txt".put("hello");
<string> f"sub:/a.txt".get();
DEFINE BUCKET escape BACKEND "global://app/../other";
DEFINE BUCKET missing;
INFO FOR DB.buckets;
