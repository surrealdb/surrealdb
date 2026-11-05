/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "Setup for 7465_value_clauses_read_as_the_caller.surql: a vault the caller can never read, and a table whose VALUE, DEFAULT, COMPUTED and ASSERT clauses each read it."
run = false
*/

DEFINE TABLE vault SCHEMALESS PERMISSIONS NONE;
CREATE vault:one SET secret = 'hunter2';
DEFINE TABLE note SCHEMALESS PERMISSIONS FULL;
DEFINE FIELD by_value ON note VALUE vault:one.secret;
DEFINE FIELD by_default ON note DEFAULT (SELECT VALUE secret FROM ONLY vault:one);
DEFINE FIELD by_computed ON note COMPUTED vault:one.secret;
DEFINE FIELD checked ON note ASSERT vault:one.secret = NONE;
