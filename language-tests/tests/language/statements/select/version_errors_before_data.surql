/**
[env]
backend = ["mem", "surrealkv"]
versioned = true
timeout = 5000

[test]
reason = "VERSION clause queries at epoch timestamp show empty schema via INFO FOR DB"

[[test.results]]
value = "[{ id: test:1, val: 1 }]"

[[test.results]]
value = "{ accesses: {  }, analyzers: {  }, apis: {  }, buckets: {  }, compatibility: 4, configs: {  }, extensions: {  }, functions: {  }, models: {  }, params: {  }, sequences: {  }, tables: {  }, types: {  }, users: {  } }"

*/

-- Create some data
CREATE test:1 SET val = 1;

-- INFO FOR DB at epoch: should show empty schema since nothing was defined yet
INFO FOR DB VERSION d'1970-01-01T00:00:01Z';
