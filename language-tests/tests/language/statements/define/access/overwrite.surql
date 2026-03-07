/**
[test]
reason = "Test DEFINE ACCESS OVERWRITE replaces an existing access method"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ accesses: { api: 'DEFINE ACCESS api ON DATABASE TYPE BEARER FOR USER DURATION FOR GRANT 1w, FOR TOKEN 1h, FOR SESSION NONE' }, analyzers: {  }, apis: {  }, buckets: {  }, configs: {  }, functions: {  }, models: {  }, modules: {  }, params: {  }, sequences: {  }, tables: {  }, users: {  } }"

*/

DEFINE ACCESS api ON DATABASE TYPE BEARER FOR USER;
DEFINE ACCESS OVERWRITE api ON DATABASE TYPE BEARER FOR USER DURATION FOR GRANT 7d;
INFO FOR DATABASE;
