/**
[env]
namespace = false
database = false
auth = { level = "owner" }

[test]
reason = "A bucket is defined at the root, a namespace or a database, and a file pointer reaches the buckets at the level of the session: a root session the root's, a session with a namespace that namespace's, a session with a database that database's."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "'root'"

[[test.results]]
value = """{ shared: "DEFINE BUCKET shared ON ROOT BACKEND 'memory' PERMISSIONS FULL" }"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ database: NONE, namespace: 'lvl' }"

[[test.results]]
error = "The bucket 'shared' does not exist"

[[test.results]]
value = "NONE"

[[test.results]]
value = "false"

[[test.results]]
value = "NONE"

[[test.results]]
value = """{ accesses: {  }, buckets: { shared: "DEFINE BUCKET shared ON NAMESPACE BACKEND 'memory' PERMISSIONS FULL" }, databases: {  }, users: {  } }"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ database: 'lvl', namespace: 'lvl' }"

[[test.results]]
value = "NONE"

[[test.results]]
value = "false"

[[test.results]]
value = """{ shared: "DEFINE BUCKET shared BACKEND 'memory' PERMISSIONS FULL" }"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The bucket 'shared' does not exist"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ accesses: {  }, buckets: {  }, databases: { lvl: 'DEFINE DATABASE lvl COMPATIBILITY 4' }, users: {  } }"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The bucket 'shared' does not exist"

*/

DEFINE BUCKET shared ON ROOT BACKEND "memory";
f"shared:/a.txt".put("root");
<string> f"shared:/a.txt".get();
INFO FOR ROOT.buckets;
DEFINE NAMESPACE lvl;
USE NS lvl;
f"shared:/a.txt".exists();
DEFINE BUCKET shared ON NAMESPACE BACKEND "memory";
f"shared:/a.txt".exists();
f"shared:/a.txt".put("namespace");
INFO FOR NS;
DEFINE DATABASE lvl;
USE DB lvl;
DEFINE BUCKET shared BACKEND "memory";
f"shared:/a.txt".exists();
(INFO FOR DB).buckets;
ALTER BUCKET shared ON NAMESPACE COMMENT "ns";
REMOVE BUCKET shared ON NAMESPACE;
REMOVE BUCKET shared ON NAMESPACE;
REMOVE BUCKET IF EXISTS shared ON NAMESPACE;
INFO FOR NS;
REMOVE BUCKET shared;
REMOVE NAMESPACE lvl;
REMOVE BUCKET shared ON ROOT;
REMOVE BUCKET shared ON ROOT;
