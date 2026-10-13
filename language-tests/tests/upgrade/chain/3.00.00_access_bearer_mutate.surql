/**
[test]
reason = "Chain mutation for access_bearer.surql: every walked version issues one grant against the BEARER access an older version defined, so each release appends to a grant keyspace its predecessors populated."
run = false
*/
ACCESS chain_access_bearer_ac ON DATABASE GRANT FOR USER chain_access_bearer_user;
