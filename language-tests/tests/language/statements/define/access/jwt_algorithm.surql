/**
[test]
reason = "Test DEFINE ACCESS with JWT type and different algorithms"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = """{ accesses: { jwt_hs256: "DEFINE ACCESS jwt_hs256 ON DATABASE TYPE JWT ALGORITHM HS256 KEY '[REDACTED]' WITH ISSUER KEY '[REDACTED]' DURATION FOR TOKEN 1h, FOR SESSION NONE", jwt_hs384: "DEFINE ACCESS jwt_hs384 ON DATABASE TYPE JWT ALGORITHM HS384 KEY '[REDACTED]' WITH ISSUER KEY '[REDACTED]' DURATION FOR TOKEN 1h, FOR SESSION NONE", jwt_hs512: "DEFINE ACCESS jwt_hs512 ON DATABASE TYPE JWT ALGORITHM HS512 KEY '[REDACTED]' WITH ISSUER KEY '[REDACTED]' DURATION FOR TOKEN 1h, FOR SESSION NONE" }, analyzers: {  }, apis: {  }, buckets: {  }, configs: {  }, functions: {  }, models: {  }, modules: {  }, params: {  }, sequences: {  }, tables: {  }, users: {  } }"""

*/

DEFINE ACCESS jwt_hs256 ON DATABASE TYPE JWT ALGORITHM HS256 KEY 'secret256';
DEFINE ACCESS jwt_hs384 ON DATABASE TYPE JWT ALGORITHM HS384 KEY 'secret384';
DEFINE ACCESS jwt_hs512 ON DATABASE TYPE JWT ALGORITHM HS512 KEY 'secret512';
INFO FOR DATABASE;
