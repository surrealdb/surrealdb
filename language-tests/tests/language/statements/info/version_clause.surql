/**
[test]
reason = "INFO FOR DB / NS accepts a VERSION clause referencing a past timestamp. With no schema changes since that timestamp, the result is an empty info object."

[env]
planner-strategy = ["all-ro", "best-effort-ro"]
# surrealkv / rocksdb refuse versioned queries unless the datastore was
# opened with `?versioned=true`; the harness passes that through when
# `versioned = true` is set on the env. Memory implicitly supports it.
versioned = true

[[test.results]]
value = "{ accesses: {  }, analyzers: {  }, apis: {  }, buckets: {  }, configs: {  }, functions: {  }, models: {  }, modules: {  }, params: {  }, sequences: {  }, tables: {  }, users: {  } }"

[[test.results]]
value = "{ accesses: {  }, databases: {  }, users: {  } }"

*/

INFO FOR DB VERSION d'2025-01-01T00:00:00Z';
INFO FOR NS VERSION d'2025-01-01T00:00:00Z';
