/**
[test]
reason = "INLINE EDGES / INLINE REFERENCES caps: DEFINE and ALTER grammar, INFO rendering only when set, DROP clauses, redefinition, the zero cap, and the hard upper bound"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ accesses: {  }, analyzers: {  }, apis: {  }, buckets: {  }, configs: {  }, functions: {  }, models: {  }, modules: {  }, params: {  }, sequences: {  }, tables: { test: 'DEFINE TABLE test TYPE ANY SCHEMALESS INLINE EDGES 64 INLINE REFERENCES 32 PERMISSIONS NONE' }, users: {  } }"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ accesses: {  }, analyzers: {  }, apis: {  }, buckets: {  }, configs: {  }, functions: {  }, models: {  }, modules: {  }, params: {  }, sequences: {  }, tables: { test: 'DEFINE TABLE test TYPE ANY SCHEMALESS INLINE EDGES 8 PERMISSIONS NONE' }, users: {  } }"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ accesses: {  }, analyzers: {  }, apis: {  }, buckets: {  }, configs: {  }, functions: {  }, models: {  }, modules: {  }, params: {  }, sequences: {  }, tables: { test: 'DEFINE TABLE test TYPE ANY SCHEMALESS PERMISSIONS NONE' }, users: {  } }"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ accesses: {  }, analyzers: {  }, apis: {  }, buckets: {  }, configs: {  }, functions: {  }, models: {  }, modules: {  }, params: {  }, sequences: {  }, tables: { test: 'DEFINE TABLE test TYPE ANY SCHEMALESS INLINE EDGES 0 PERMISSIONS NONE' }, users: {  } }"

[[test.results]]
error = "An error occurred: an INLINE cache cap cannot exceed 256"

[[test.results]]
error = "An error occurred: an INLINE cache cap cannot exceed 256"
*/

-- Both caps on one definition; INFO renders them after the GraphQL clauses.
DEFINE TABLE test INLINE EDGES 64 INLINE REFERENCES 32;
INFO FOR DB;

-- ALTER can set one cap and drop the other in a single statement.
ALTER TABLE test INLINE EDGES 8 DROP INLINE REFERENCES;
INFO FOR DB;

-- Dropping the last cap removes the clause from the rendering entirely.
ALTER TABLE test DROP INLINE EDGES;
INFO FOR DB;

-- A zero cap is stored and rendered as written (it disables the cache,
-- exactly like an absent cap).
DEFINE TABLE OVERWRITE test INLINE EDGES 0;
INFO FOR DB;

-- The caps are bounded: every entry write rewrites the whole cache value.
DEFINE TABLE bad INLINE EDGES 257;
ALTER TABLE test INLINE REFERENCES 300;
