/**
[test]
reason = "DEFINE ACCESS TYPE JWT with an AUDIENCE clause stores and reports the audience list"

[[test.results]]
value = "NONE"

[[test.results]]
value = """{ accesses: { testac: "DEFINE ACCESS testac ON DATABASE TYPE JWT ALGORITHM HS512 KEY '[REDACTED]' AUDIENCE 'aud1', 'aud2' WITH ISSUER KEY '[REDACTED]' DURATION FOR TOKEN 1h, FOR SESSION NONE" }, analyzers: {  }, apis: {  }, buckets: {  }, configs: {  }, functions: {  }, models: {  }, modules: {  }, params: {  }, sequences: {  }, tables: {  }, users: {  } }"""

[[test.results]]
value = "[{ duration: { session: NONE, token: 1h }, kind: { jwt: { audience: ['aud1', 'aud2'], issuer: { alg: 'HS512', key: '[REDACTED]' }, verify: { alg: 'HS512', key: '[REDACTED]' } }, kind: 'JWT' }, name: 'testac' }]"

*/

DEFINE ACCESS testac ON DATABASE TYPE JWT ALGORITHM HS512 KEY "secret" AUDIENCE "aud1", "aud2";

INFO FOR DB;

RETURN (INFO FOR DB STRUCTURE).accesses;
