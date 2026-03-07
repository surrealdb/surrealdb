/**
[test]
reason = "Test DEFINE ACCESS at different base scopes (ROOT, NAMESPACE, DATABASE) to exercise Base enum"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ accesses: { api_db: 'DEFINE ACCESS api_db ON DATABASE TYPE BEARER FOR USER DURATION FOR GRANT 4w2d, FOR TOKEN 1h, FOR SESSION NONE' }, analyzers: {  }, apis: {  }, buckets: {  }, configs: {  }, functions: {  }, models: {  }, modules: {  }, params: {  }, sequences: {  }, tables: {  }, users: {  } }"

[[test.results]]
value = "{ accesses: { api_ns: 'DEFINE ACCESS api_ns ON NAMESPACE TYPE BEARER FOR USER DURATION FOR GRANT 4w2d, FOR TOKEN 1h, FOR SESSION NONE' }, databases: { test: 'DEFINE DATABASE test' }, users: {  } }"

*/

DEFINE ACCESS api_db ON DATABASE TYPE BEARER FOR USER;
DEFINE ACCESS api_ns ON NAMESPACE TYPE BEARER FOR USER;
INFO FOR DATABASE;
INFO FOR NAMESPACE;
