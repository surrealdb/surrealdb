/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "Both INFO FOR DB shapes expose a `modules` entry -- an empty object in the default form, an empty array in the STRUCTURE form -- positioned between `models` and `params`. The entry is not compiled conditionally, so it is present even in a build that omits the `surrealism` cargo feature, as this one does."

[[test.results]]
value = "{ accesses: {  }, analyzers: {  }, apis: {  }, buckets: {  }, configs: {  }, functions: {  }, models: {  }, modules: {  }, params: {  }, sequences: {  }, tables: {  }, users: {  } }"

[[test.results]]
value = "{ accesses: [], analyzers: [], apis: [], buckets: [], configs: [], functions: [], models: [], modules: [], params: [], sequences: [], tables: [], users: [] }"

*/

INFO FOR DB;
INFO FOR DB STRUCTURE;
