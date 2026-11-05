/**
[env]
remote-exclusive = true
imports = ["upgrade/define/3.00.00_access_scopes_import.surql"]
clean = true

[test]
upgrade = true
reason = """
Covers the namespace and root access keyspaces: a JWT access an older binary
stored outside a database must still be found, decoded, and addressable after
the upgrade.

The new binary must read both scopes back with algorithm, both durations and
the comment intact, must keep them out of the database scope, and must still be
able to address the exact rows the old binary wrote — a redefinition is refused
as a duplicate and a REMOVE at the same scope deletes them.

Key material is not asserted: INFO renders every symmetric key as [REDACTED],
so the rendered text carries no evidence about the stored secret either way."""

[[test.results]]
match = '''$result.accesses.up_access_scopes_ns == "DEFINE ACCESS up_access_scopes_ns ON NAMESPACE TYPE JWT ALGORITHM HS512 KEY '[REDACTED]' WITH ISSUER KEY '[REDACTED]' DURATION FOR TOKEN 30m, FOR SESSION 12h COMMENT 'namespace jwt access'"'''

[[test.results]]
match = '''$result.accesses.up_access_scopes_root == "DEFINE ACCESS up_access_scopes_root ON ROOT TYPE JWT ALGORITHM HS256 KEY '[REDACTED]' WITH ISSUER KEY '[REDACTED]' DURATION FOR TOKEN 15m, FOR SESSION 6h COMMENT 'root jwt access'"'''

[[test.results]]
match = '''
    $result.accesses.up_access_scopes_ns == NONE
        && $result.accesses.up_access_scopes_root == NONE
'''

[[test.results]]
error = "The access method 'up_access_scopes_ns' already exists in the namespace 'test'"

[[test.results]]
error = "The root access method 'up_access_scopes_root' already exists"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
match = '''$result.accesses.up_access_scopes_ns == NONE'''

[[test.results]]
match = '''$result.accesses.up_access_scopes_root == NONE'''

*/
INFO FOR NAMESPACE;
INFO FOR ROOT;
-- The two accesses live outside the database, so the database scope must not
-- report them.
INFO FOR DATABASE;
-- A redefinition without OVERWRITE is refused, which is only possible if the
-- new binary's existence check reads the row the old binary wrote.
DEFINE ACCESS up_access_scopes_ns ON NAMESPACE TYPE JWT ALGORITHM HS512 KEY 'namespace-shared-secret';
DEFINE ACCESS up_access_scopes_root ON ROOT TYPE JWT ALGORITHM HS256 KEY 'root-shared-secret';
-- REMOVE reconstructs the key from the name and scope, so a successful delete
-- proves the new binary addresses the same key the old binary wrote to.
REMOVE ACCESS up_access_scopes_ns ON NAMESPACE;
REMOVE ACCESS up_access_scopes_root ON ROOT;
INFO FOR NAMESPACE;
INFO FOR ROOT;
