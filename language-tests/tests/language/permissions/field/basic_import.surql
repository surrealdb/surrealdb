/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ create: 1, full: 1, id: full:filled, none: 1, select: 1, update: 1 }]"

[[test.results]]
value = "[{ id: full:empty }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ create: 1, full: 1, id: `none`:filled, none: 1, select: 1, update: 1 }]"

[[test.results]]
value = "[{ id: `none`:empty }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ create: 1, full: 1, id: select:filled, none: 1, select: 1, update: 1 }]"

[[test.results]]
value = "[{ id: select:empty }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ create: 1, full: 1, id: create:filled, none: 1, select: 1, update: 1 }]"

[[test.results]]
value = "[{ id: create:empty }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ create: 1, full: 1, id: update:filled, none: 1, select: 1, update: 1 }]"

[[test.results]]
value = "[{ id: update:empty }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = """{ accesses: { test: "DEFINE ACCESS test ON DATABASE TYPE RECORD WITH JWT ALGORITHM HS512 KEY '[REDACTED]' WITH ISSUER KEY '[REDACTED]' DURATION FOR TOKEN 1h, FOR SESSION NONE" }, analyzers: {  }, apis: {  }, buckets: {  }, configs: {  }, functions: {  }, models: {  }, modules: {  }, params: {  }, sequences: {  }, tables: { create: 'DEFINE TABLE `create` TYPE ANY SCHEMALESS PERMISSIONS FOR select, update, delete NONE, FOR create FULL', full: 'DEFINE TABLE full TYPE ANY SCHEMALESS PERMISSIONS FULL', none: 'DEFINE TABLE `none` TYPE ANY SCHEMALESS PERMISSIONS NONE', select: 'DEFINE TABLE `select` TYPE ANY SCHEMALESS PERMISSIONS FOR select FULL, FOR create, update, delete NONE', update: 'DEFINE TABLE `update` TYPE ANY SCHEMALESS PERMISSIONS FOR select, create, delete NONE, FOR update FULL' }, users: {  } }"""

[[test.results]]
value = "{ events: {  }, fields: {  }, indexes: {  }, lives: {  }, tables: {  } }"

*/
DEFINE TABLE full PERMISSIONS FULL;

DEFINE FIELD full ON full PERMISSIONS FULL;
DEFINE FIELD `none` ON full PERMISSIONS NONE;
DEFINE FIELD `select` ON full PERMISSIONS FOR select FULL FOR create, update NONE;
DEFINE FIELD `create` ON full PERMISSIONS FOR create FULL FOR select, update NONE;
DEFINE FIELD `update` ON full PERMISSIONS FOR update FULL FOR select, create NONE;

CREATE full:filled SET full = 1, `none` = 1, `select` = 1, `create` = 1, `update` = 1;
CREATE full:empty;


DEFINE TABLE `none` PERMISSIONS NONE;

DEFINE FIELD full ON `none` PERMISSIONS FULL;
DEFINE FIELD `none` ON `none` PERMISSIONS NONE;
DEFINE FIELD `select` ON `none` PERMISSIONS FOR select FULL FOR create, update NONE;
DEFINE FIELD `create` ON `none` PERMISSIONS FOR create FULL FOR select, update NONE;
DEFINE FIELD `update` ON `none` PERMISSIONS FOR update FULL FOR select, create NONE;

CREATE `none`:filled SET full = 1, `none` = 1, `select` = 1, `create` = 1, `update` = 1;
CREATE `none`:empty;


DEFINE TABLE `select` PERMISSIONS FOR select FULL;

DEFINE FIELD full ON `select` PERMISSIONS FULL;
DEFINE FIELD `none` ON `select` PERMISSIONS NONE;
DEFINE FIELD `select` ON `select` PERMISSIONS FOR select FULL FOR create, update NONE;
DEFINE FIELD `create` ON `select` PERMISSIONS FOR create FULL FOR select, update NONE;
DEFINE FIELD `update` ON `select` PERMISSIONS FOR update FULL FOR select, create NONE;

CREATE `select`:filled SET full = 1, `none` = 1, `select` = 1, `create` = 1, `update` = 1;
CREATE `select`:empty;


DEFINE TABLE `create` PERMISSIONS FOR create FULL;

DEFINE FIELD full ON `create` PERMISSIONS FULL;
DEFINE FIELD `none` ON `create` PERMISSIONS NONE;
DEFINE FIELD `select` ON `create` PERMISSIONS FOR select FULL FOR create, update NONE;
DEFINE FIELD `create` ON `create` PERMISSIONS FOR create FULL FOR select, update NONE;
DEFINE FIELD `update` ON `create` PERMISSIONS FOR update FULL FOR select, create NONE;

CREATE `create`:filled SET full = 1, `none` = 1, `select` = 1, `create` = 1, `update` = 1;
CREATE `create`:empty;


DEFINE TABLE `update` PERMISSIONS FOR update FULL;

DEFINE FIELD full ON `update` PERMISSIONS FULL;
DEFINE FIELD `none` ON `update` PERMISSIONS NONE;
DEFINE FIELD `select` ON `update` PERMISSIONS FOR select FULL FOR create, update NONE;
DEFINE FIELD `create` ON `update` PERMISSIONS FOR create FULL FOR select, update NONE;
DEFINE FIELD `update` ON `update` PERMISSIONS FOR update FULL FOR select, create NONE;

CREATE `update`:filled SET full = 1, `none` = 1, `select` = 1, `create` = 1, `update` = 1;
CREATE `update`:empty;


DEFINE ACCESS test ON DATABASE;

INFO FOR DB;
INFO FOR TABLE t;

