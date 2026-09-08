/**
[test]

[[test.results]]
value = "NONE"

*/
DEFINE ACCESS api_jwt ON DATABASE TYPE JWT
    ALGORITHM HS256 KEY 'some-shared-secret'
    WITH ISSUER KEY 'some-shared-secret'
    DURATION FOR TOKEN 1h, FOR SESSION 2h
    COMMENT 'jwt access for api';
