/**
[test]
reason = "Test DEFINE ACCESS IF NOT EXISTS does not replace an existing access method"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ accesses: { api: 'DEFINE ACCESS api ON DATABASE TYPE BEARER FOR USER DURATION FOR GRANT 4w2d, FOR TOKEN 1h, FOR SESSION NONE' }, analyzers: {  }, apis: {  }, buckets: {  }, configs: {  }, functions: {  }, models: {  }, modules: {  }, params: {  }, sequences: {  }, tables: {  }, users: {  } }"

*/

DEFINE ACCESS api ON DATABASE TYPE BEARER FOR USER;
DEFINE ACCESS IF NOT EXISTS api ON DATABASE TYPE BEARER FOR USER DURATION FOR GRANT 7d;
INFO FOR DATABASE;
