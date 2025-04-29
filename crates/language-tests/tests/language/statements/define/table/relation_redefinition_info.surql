/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ events: {  }, fields: { in: 'DEFINE FIELD in ON likes TYPE record<person> PERMISSIONS FULL', out: 'DEFINE FIELD out ON likes TYPE record<person> PERMISSIONS FULL' }, indexes: {  }, lives: {  }, tables: {  } }"

[[test.results]]
value = "{ accesses: {  }, analyzers: {  }, apis: {  }, buckets: {  }, configs: {  }, functions: {  }, models: {  }, params: {  }, sequences: {  }, tables: { likes: 'DEFINE TABLE likes TYPE RELATION IN person OUT person SCHEMALESS PERMISSIONS NONE' }, users: {  } }"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ events: {  }, fields: { in: 'DEFINE FIELD in ON likes TYPE record<person> PERMISSIONS FULL', out: 'DEFINE FIELD out ON likes TYPE record<person | thing> PERMISSIONS FULL' }, indexes: {  }, lives: {  }, tables: {  } }"

[[test.results]]
value = "{ accesses: {  }, analyzers: {  }, apis: {  }, buckets: {  }, configs: {  }, functions: {  }, models: {  }, params: {  }, sequences: {  }, tables: { likes: 'DEFINE TABLE likes TYPE RELATION IN person OUT person | thing SCHEMALESS PERMISSIONS NONE' }, users: {  } }"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ events: {  }, fields: { in: 'DEFINE FIELD in ON likes TYPE record<person> PERMISSIONS FULL', out: 'DEFINE FIELD out ON likes TYPE record<person | thing | other> PERMISSIONS FULL' }, indexes: {  }, lives: {  }, tables: {  } }"

[[test.results]]
value = "{ accesses: {  }, analyzers: {  }, apis: {  }, buckets: {  }, configs: {  }, functions: {  }, models: {  }, params: {  }, sequences: {  }, tables: { likes: 'DEFINE TABLE likes TYPE RELATION IN person OUT person | thing | other SCHEMALESS PERMISSIONS NONE' }, users: {  } }"

*/

DEFINE TABLE likes TYPE RELATION IN person OUT person;
INFO FOR TABLE likes;
INFO FOR DB;
REMOVE TABLE likes;
DEFINE TABLE likes TYPE RELATION IN person OUT person | thing;
INFO FOR TABLE likes;
INFO FOR DB;
REMOVE FIELD out ON TABLE likes;
DEFINE FIELD out ON TABLE likes TYPE record<person | thing | other>;
INFO FOR TABLE likes;
INFO FOR DB;
