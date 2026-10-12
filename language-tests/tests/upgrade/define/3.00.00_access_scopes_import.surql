/**
[env]
remote-exclusive = true
# Root- and namespace-scoped state outlives a REMOVE NAMESPACE, so the ordinary
# runner has to be told to reset the shared datastore after this test. The
# upgrade runner ignores the flag — it gives every run its own store directory.
clean = true

[test]
reason = "Setup for access_scopes.surql: the old binary writes a JWT access into the namespace access keyspace and another into the root access keyspace."
run = false

*/
DEFINE ACCESS up_access_scopes_ns ON NAMESPACE TYPE JWT
    ALGORITHM HS512 KEY 'namespace-shared-secret'
    WITH ISSUER KEY 'namespace-shared-secret'
    DURATION FOR TOKEN 30m, FOR SESSION 12h
    COMMENT 'namespace jwt access';
DEFINE ACCESS up_access_scopes_root ON ROOT TYPE JWT
    ALGORITHM HS256 KEY 'root-shared-secret'
    WITH ISSUER KEY 'root-shared-secret'
    DURATION FOR TOKEN 15m, FOR SESSION 6h
    COMMENT 'root jwt access';
