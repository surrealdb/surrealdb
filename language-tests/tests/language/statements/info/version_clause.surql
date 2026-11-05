/**
[test]
reason = "INFO FOR DB / NS accepts a VERSION clause referencing a past timestamp. With no schema changes since that timestamp, the result is an empty info object."

[env]
# Excludes tikv: the TiKV backend does not support versioned / time-travel
# queries (it returns UnsupportedVersionedQueries), so a VERSION clause
# cannot be exercised there.
backend = ["mem", "surrealkv"]
# surrealkv refuses versioned queries unless the datastore was
# opened with `?versioned=true`; the harness passes that through when
# `versioned = true` is set on the env. Memory cannot serve them at all, so
# the runner skips this test there.
versioned = true

[[test.results]]
value = "{ accesses: {  }, analyzers: {  }, apis: {  }, buckets: {  }, compatibility: 4, configs: {  }, extensions: {  }, functions: {  }, models: {  }, params: {  }, sequences: {  }, tables: {  }, types: {  }, users: {  } }"

[[test.results]]
value = "{ accesses: {  }, databases: {  }, users: {  } }"

*/

INFO FOR DB VERSION d'2025-01-01T00:00:00Z';
INFO FOR NS VERSION d'2025-01-01T00:00:00Z';
