/**
[test]
run = false
*/

-- Setup: define a DB-level owner user and a root-defined function
DEFINE USER db_owner ON DATABASE PASSWORD 'db_owner' ROLES OWNER;
DEFINE FUNCTION fn::escalate() { RETURN 'safe'; };
