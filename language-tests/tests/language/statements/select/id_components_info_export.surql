/**
[test]
reason = """
	INFO FOR TABLE shows the record id as written and none of the fields it
	derives from it, since the components are not definitions of their own;
	INFO ... STRUCTURE adds the components to the id.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ events: {  }, fields: { id: 'DEFINE FIELD id ON reading TYPE [city: string, time: datetime] PERMISSIONS FULL', temp: 'DEFINE FIELD temp ON reading TYPE int PERMISSIONS FULL' }, indexes: {  }, lives: {  }, tables: {  } }"

[[test.results]]
value = "{ events: [ ], fields: [ { components: [ { kind: 'string', name: 'city', position: 0 }, { kind: 'datetime', name: 'time', position: 1 } ], kind: '[string, datetime]', name: 'id', permissions: { create: true, select: true, update: true }, readonly: false, table: 'reading' }, { kind: 'int', name: 'temp', permissions: { create: true, select: true, update: true }, readonly: false, table: 'reading' } ], indexes: [ ], lives: [ ], tables: [ ] }"
*/

DEFINE TABLE reading SCHEMAFULL;
DEFINE FIELD id ON reading TYPE [city: string, time: datetime];
DEFINE FIELD temp ON reading TYPE int;

INFO FOR TABLE reading;
INFO FOR TABLE reading STRUCTURE;
