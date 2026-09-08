/**
[test]
reason = "Chain setup for access_bearer.surql: the first walked version defines the database user and the BEARER access every later version issues grants against, and issues the first grant in the set, revoking it immediately so the walk has a revocation written by its oldest binary to re-read at every later hop."
run = false
*/
DEFINE USER chain_access_bearer_user ON DATABASE PASSWORD 'chain-bearer-pw' ROLES VIEWER;
DEFINE ACCESS chain_access_bearer_ac ON DATABASE TYPE BEARER FOR USER
    DURATION FOR GRANT 4w2d, FOR TOKEN 1h, FOR SESSION NONE;
-- The first grant in the set, revoked while it is the only one so `ALL` names
-- exactly it. Every later hop appends a live grant and reads the whole set back,
-- so the revocation stamped here has to survive every release in turn.
ACCESS chain_access_bearer_ac ON DATABASE GRANT FOR USER chain_access_bearer_user;
ACCESS chain_access_bearer_ac ON DATABASE REVOKE ALL;
