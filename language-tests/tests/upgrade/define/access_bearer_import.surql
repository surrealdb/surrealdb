/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

*/
DEFINE USER api_user ON DATABASE PASSWORD 'pw' ROLES VIEWER;
DEFINE ACCESS api_bearer ON DATABASE TYPE BEARER FOR USER
    DURATION FOR GRANT 4w2d, FOR TOKEN 1h, FOR SESSION NONE;
