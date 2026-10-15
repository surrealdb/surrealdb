/**
[env]
remote-exclusive = true
clean = true

[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ database: '', namespace: '' }"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = '''{ accesses: {  }, databases: { "": 'DEFINE DATABASE `` COMPATIBILITY 3' }, users: {  } }'''

[[test.results]]
value = '''{ accesses: {  }, analyzers: {  }, apis: {  }, buckets: {  }, compatibility: 3, configs: {  }, extensions: {  }, functions: {  }, models: {  }, params: {  }, sequences: {  }, tables: { "": 'DEFINE TABLE `` TYPE NORMAL SCHEMAFULL PERMISSIONS NONE' }, users: {  } }'''

[[test.results]]
value = '''{ events: {  }, fields: { "``": 'DEFINE FIELD `` ON `` TYPE object PERMISSIONS FULL', "``.``": 'DEFINE FIELD ``.`` ON `` TYPE number PERMISSIONS FULL' }, indexes: {  }, lives: {  }, tables: {  } }'''

[[test.results]]
value = "[{ ``: { ``: 1 }, id: ``:1 }]"

[[test.results]]
value = "[{ ``: { ``: 2 }, id: ``:2 }]"

[[test.results]]
value = "[{ ``: { ``: 3 }, id: ``:`` }]"

[[test.results]]
value = "[{ ``: { ``: 1 }, id: ``:1 }, { ``: { ``: 2 }, id: ``:2 }, { ``: { ``: 3 }, id: ``:`` }]"

*/

-- The database of namespace `` is created implicitly, at the store's
-- default level. At level 4 `INFO FOR TABLE` on the SCHEMAFULL table also
-- shows its migrations.
ALTER SYSTEM COMPATIBILITY 3;

DEFINE NAMESPACE ``;
DEFINE DATABASE ``;

USE NS `` DB ``;

DEFINE TABLE `` SCHEMAFULL;

DEFINE FIELD `` ON `` TYPE object;
DEFINE FIELD ``.`` ON `` TYPE number;

INFO FOR NAMESPACE;
INFO FOR DATABASE;
INFO FOR TB ``;

CREATE ``:1 SET `` = { "": 1 };
CREATE ``:2 SET `` = { "": 2 };
CREATE ``:`` SET `` = { "": 3 };

SELECT * FROM ``;
