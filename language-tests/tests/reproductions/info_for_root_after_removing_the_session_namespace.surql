/**
[env]
namespace = false
database = false
clean = true

[test]
reason = """
A root-level statement needs no namespace, so a session whose selected
namespace no longer exists still runs it. INFO FOR ROOT after REMOVE NAMESPACE
of the namespace the session is on answers the root info, and the removed
namespace is absent from it, whether or not the session also selects a database
in it.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ database: NONE, namespace: 'dbg' }"

[[test.results]]
value = "NONE"

[[test.results]]
match = "$result.namespaces.dbg == NONE"
error = false

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ database: 'dbg', namespace: 'dbg' }"

[[test.results]]
value = "NONE"

[[test.results]]
match = "$result.namespaces.dbg == NONE"
error = false
*/

DEFINE NAMESPACE dbg;
USE NS dbg;
REMOVE NAMESPACE dbg;
INFO FOR ROOT;

DEFINE NAMESPACE dbg;
USE NS dbg DB dbg;
REMOVE NAMESPACE dbg;
INFO FOR ROOT;
