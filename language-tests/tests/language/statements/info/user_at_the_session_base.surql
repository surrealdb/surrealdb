/**
[env]
namespace = false
database = false
clean = true

[test]
reason = "INFO FOR USER with no ON clause reads the user at the base the session selects when it runs — the root, its namespace or its database — inside a block as at the top level, and raises the not-found error of that base for a user, namespace or database that does not exist"

[[test.results]]
value = "NONE"

[[test.results]]
value = """"DEFINE USER rooted ON ROOT PASSHASH '[REDACTED]' PASSSCRAM '[REDACTED]' ROLES VIEWER DURATION FOR TOKEN 1h, FOR SESSION NONE""""

[[test.results]]
error = "The root user 'nobody' does not exist"

[[test.results]]
value = "{ database: NONE, namespace: 'ghost' }"

[[test.results]]
error = "The user 'spaced' does not exist in the namespace 'ghost'"

[[test.results]]
value = "{ database: NONE, namespace: 'ns' }"

[[test.results]]
value = "NONE"

[[test.results]]
value = """"DEFINE USER spaced ON NAMESPACE PASSHASH '[REDACTED]' PASSSCRAM '[REDACTED]' ROLES VIEWER DURATION FOR TOKEN 1h, FOR SESSION NONE""""

[[test.results]]
error = "The user 'rooted' does not exist in the namespace 'ns'"

[[test.results]]
value = "{ database: 'db', namespace: 'ns' }"

[[test.results]]
value = "NONE"

[[test.results]]
value = "['EDITOR']"

[[test.results]]
value = "'based'"

[[test.results]]
value = "'based'"

[[test.results]]
error = "The user 'spaced' does not exist in the database 'db'"

[[test.results]]
value = "{ database: 'nowhere', namespace: 'ghost' }"

[[test.results]]
error = "The user 'based' does not exist in the database 'nowhere'"

[[test.results]]
value = "{ database: 'nowhere', namespace: 'ns' }"

[[test.results]]
error = "The user 'based' does not exist in the database 'nowhere'"

*/

DEFINE USER rooted ON ROOT PASSWORD 'secret' ROLES VIEWER;
INFO FOR USER rooted;
INFO FOR USER nobody;

USE NS ghost;
INFO FOR USER spaced;

USE NS ns;
DEFINE USER spaced ON NS PASSWORD 'secret' ROLES VIEWER;
INFO FOR USER spaced;
INFO FOR USER rooted;

USE NS ns DB db;
DEFINE USER based ON DATABASE PASSWORD 'secret' ROLES EDITOR;
RETURN (INFO FOR USER based STRUCTURE).roles;
{ LET $u = INFO FOR USER based STRUCTURE; RETURN $u.name; };
FOR $name IN ['based'] { RETURN (INFO FOR USER $name STRUCTURE).name; };
INFO FOR USER spaced;

USE NS ghost DB nowhere;
INFO FOR USER based;

USE NS ns DB nowhere;
INFO FOR USER based;
