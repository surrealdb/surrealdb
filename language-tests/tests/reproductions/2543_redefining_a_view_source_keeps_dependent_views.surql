/**
[env]
timeout = 5000

[test]
reason = "A view over a view keeps reading it when the middle view is redefined over another table: INFO and writes both follow the redefinition"
issue = 2543

[[test.results]]
value = "[{ id: x:1, x_value: 1 }]"

[[test.results]]
value = "[{ id: y:1, y_value: 1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ y_value: 1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ x_value: 1, y_value: NONE }]"

[[test.results]]
value = "[{ x_value: 1, y_value: NONE }]"

[[test.results]]
value = "{ events: {  }, fields: {  }, indexes: {  }, lives: {  }, tables: { z: 'DEFINE TABLE z TYPE ANY SCHEMALESS AS SELECT * FROM y PERMISSIONS NONE' } }"

[[test.results]]
value = "{ accesses: {  }, analyzers: {  }, apis: {  }, buckets: {  }, compatibility: 4, configs: {  }, extensions: {  }, functions: {  }, models: {  }, params: {  }, sequences: {  }, tables: { x: 'DEFINE TABLE x TYPE ANY SCHEMALESS PERMISSIONS NONE', y: 'DEFINE TABLE y TYPE ANY SCHEMALESS AS SELECT * FROM x PERMISSIONS NONE', z: 'DEFINE TABLE z TYPE ANY SCHEMALESS AS SELECT * FROM y PERMISSIONS NONE' }, users: {  } }"

[[test.results]]
value = "[{ id: x:2, x_value: 2 }]"

[[test.results]]
value = "[{ x_value: 1, y_value: NONE }, { x_value: 2, y_value: NONE }]"

[[test.results]]
value = "[{ x_value: 1, y_value: NONE }, { x_value: 2, y_value: NONE }]"

*/

CREATE x:1 SET x_value = 1;
CREATE y:1 SET y_value = 1;
DEFINE TABLE z AS SELECT * FROM y;
SELECT y_value FROM z;
DEFINE TABLE OVERWRITE y AS SELECT * FROM x;
SELECT x_value, y_value FROM y;
SELECT x_value, y_value FROM z;
INFO FOR TABLE y;
INFO FOR DB;
CREATE x:2 SET x_value = 2;
SELECT x_value, y_value FROM y ORDER BY x_value;
SELECT x_value, y_value FROM z ORDER BY x_value;
