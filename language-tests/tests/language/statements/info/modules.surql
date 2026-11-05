/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "Both INFO FOR DB shapes expose an `extensions` entry, positioned between `configs` and `functions` -- an empty object in the default form and an empty array in the STRUCTURE form -- and no `modules` entry. The entry is not compiled conditionally, so it is present even in a build that omits the `surrealism` cargo feature, as this one does."

[[test.results]]
value = "{ accesses: {  }, analyzers: {  }, apis: {  }, buckets: {  }, compatibility: 4, configs: {  }, extensions: {  }, functions: {  }, models: {  }, params: {  }, sequences: {  }, tables: {  }, types: {  }, users: {  } }"

[[test.results]]
value = "{ accesses: [], analyzers: [], apis: [], buckets: [], compatibility: 4, configs: [], extensions: [], functions: [], models: [], params: [], sequences: [], tables: [], types: [], users: [] }"

*/

INFO FOR DB;
INFO FOR DB STRUCTURE;
