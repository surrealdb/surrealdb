/**
[test]
reason = "RATELIMIT clause grammar round-trips through INFO FOR DB"

[[test.results]]
value = "NONE"

[[test.results]]
value = """{ accesses: {  }, analyzers: {  }, apis: {  }, buckets: {  }, configs: {  }, functions: {  }, models: {  }, modules: {  }, params: {  }, sequences: {  }, tables: { post: "DEFINE TABLE post TYPE ANY SCHEMALESS PERMISSIONS NONE RATELIMIT FOR SELECT WHERE $auth != NONE BY $session.ip LIMIT 2 PER 1s MAX 4, FOR UPDATE BY 'global' LIMIT 100 PER 1m" }, users: {  } }"""
*/

DEFINE TABLE post RATELIMIT FOR SELECT WHERE $auth != NONE BY $session.ip LIMIT 2 PER 1s MAX 4, FOR UPDATE BY 'global' LIMIT 100 PER 1m;
INFO FOR DB;
