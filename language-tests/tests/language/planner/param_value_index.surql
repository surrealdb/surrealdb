/**
[env]
new-planner-strategy = "compute-only"

[test]
reason = "Test planner recognition of parameter, datetime, and UUID values in WHERE with index"

[[test.results]]
value = "[{ company: 'SurrealDB', genre: 'm', id: person:tobie, name: 'Tobie' }]"

[[test.results]]
value = "[{ company: 'SurrealDB', genre: 'm', id: person:jaime, name: 'Jaime' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ detail: { plan: { index: 'name', operator: '=', value: 'Tobie' }, table: 'person' }, operation: 'Iterate Index' }, { detail: { type: 'Memory' }, operation: 'Collector' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ created_at: d'2023-12-25T17:13:01.940183014Z', id: test_user:1 }]"

[[test.results]]
value = "[{ detail: { plan: { index: 'createdAt', operator: '=', value: d'2023-12-25T17:13:01.940183014Z' }, table: 'test_user' }, operation: 'Iterate Index' }, { detail: { type: 'Memory' }, operation: 'Collector' }]"

[[test.results]]
value = "[{ detail: { plan: { index: 'createdAt', operator: '=', value: d'2023-12-25T17:13:01.940183014Z' }, table: 'test_user' }, operation: 'Iterate Index' }, { detail: { type: 'Memory' }, operation: 'Collector' }]"

[[test.results]]
value = "[{ created_at: d'2023-12-25T17:13:01.940183014Z', id: test_user:1 }]"

[[test.results]]
value = "[{ created_at: d'2023-12-25T17:13:01.940183014Z', id: test_user:1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: sessions:1, sessionUid: u'00ad70db-f435-442e-9012-1cd853102084' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ detail: { plan: { index: 'sessionUid', operator: '=', value: u'00ad70db-f435-442e-9012-1cd853102084' }, table: 'sessions' }, operation: 'Iterate Index' }, { detail: { type: 'Memory' }, operation: 'Collector' }]"

[[test.results]]
value = "[{ detail: { plan: { index: 'sessionUid', operator: '=', value: u'00ad70db-f435-442e-9012-1cd853102084' }, table: 'sessions' }, operation: 'Iterate Index' }, { detail: { type: 'Memory' }, operation: 'Collector' }]"

[[test.results]]
value = "[{ id: sessions:1, sessionUid: u'00ad70db-f435-442e-9012-1cd853102084' }]"

[[test.results]]
value = "[{ id: sessions:1, sessionUid: u'00ad70db-f435-442e-9012-1cd853102084' }]"
*/

-- Idiom param value
CREATE person:tobie SET name = 'Tobie', genre='m', company='SurrealDB';
CREATE person:jaime SET name = 'Jaime', genre='m', company='SurrealDB';
DEFINE INDEX name ON TABLE person COLUMNS name UNIQUE;
LET $name = 'Tobie';
LET $nameObj = {name:'Tobie'};
SELECT name FROM person WHERE name = $nameObj.name EXPLAIN;

-- Datetime value
DEFINE FIELD created_at ON TABLE test_user TYPE datetime;
DEFINE INDEX createdAt ON TABLE test_user COLUMNS created_at;
LET $now = d'2023-12-25T17:13:01.940183014Z';
CREATE test_user:1 CONTENT { created_at: $now };
SELECT * FROM test_user WHERE created_at = $now EXPLAIN;
SELECT * FROM test_user WHERE created_at = d'2023-12-25T17:13:01.940183014Z' EXPLAIN;
SELECT * FROM test_user WHERE created_at = $now;
SELECT * FROM test_user WHERE created_at = d'2023-12-25T17:13:01.940183014Z';

-- UUID value
DEFINE INDEX sessionUid ON sessions FIELDS sessionUid;
CREATE sessions:1 CONTENT { sessionUid: u'00ad70db-f435-442e-9012-1cd853102084' };
LET $sess = { uuid: u'00ad70db-f435-442e-9012-1cd853102084' };
SELECT * FROM sessions WHERE sessionUid = u'00ad70db-f435-442e-9012-1cd853102084' EXPLAIN;
SELECT * FROM sessions WHERE sessionUid = $sess.uuid EXPLAIN;
SELECT * FROM sessions WHERE sessionUid = u'00ad70db-f435-442e-9012-1cd853102084';
SELECT * FROM sessions WHERE sessionUid = $sess.uuid;
