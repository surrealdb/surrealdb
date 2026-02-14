/**
[test]
reason = "Test planner recognition of parameter, datetime, and UUID values in WHERE with index (new executor)"

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
value = ''''Project [ctx: Db] {rows: 1, batches: 1}
└────> Scan [ctx: Db] [source: person, predicate: name = $nameObj.name] {rows: 1, batches: 1}
         actual: [access_method: TableScan, direction: Forward]

Total rows: 1''''

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ created_at: d'2023-12-25T17:13:01.940183014Z', id: test_user:1 }]"

[[test.results]]
value = ''''Project [ctx: Db] {rows: 1, batches: 1}
└────> Scan [ctx: Db] [source: test_user, predicate: created_at = $now] {rows: 1, batches: 1}
         actual: [access_method: TableScan, direction: Forward]

Total rows: 1''''

[[test.results]]
value = '''"Project [ctx: Db] {rows: 1, batches: 1}
└────> Scan [ctx: Db] [source: test_user, predicate: created_at = d'2023-12-25T17:13:01.940183014Z'] {rows: 1, batches: 1}
         actual: [access_method: IndexScan, index: createdAt, access: Equality(d'2023-12-25T17:13:01.940183014Z'), direction: Forward]

Total rows: 1"'''

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
value = '''"Project [ctx: Db] {rows: 1, batches: 1}
└────> Scan [ctx: Db] [source: sessions, predicate: sessionUid = u'00ad70db-f435-442e-9012-1cd853102084'] {rows: 1, batches: 1}
         actual: [access_method: IndexScan, index: sessionUid, access: Equality(u'00ad70db-f435-442e-9012-1cd853102084'), direction: Forward]

Total rows: 1"'''

[[test.results]]
value = ''''Project [ctx: Db] {rows: 1, batches: 1}
└────> Scan [ctx: Db] [source: sessions, predicate: sessionUid = $sess.uuid] {rows: 1, batches: 1}
         actual: [access_method: TableScan, direction: Forward]

Total rows: 1''''

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
EXPLAIN ANALYZE SELECT name FROM person WHERE name = $nameObj.name;

-- Datetime value
DEFINE FIELD created_at ON TABLE test_user TYPE datetime;
DEFINE INDEX createdAt ON TABLE test_user COLUMNS created_at;
LET $now = d'2023-12-25T17:13:01.940183014Z';
CREATE test_user:1 CONTENT { created_at: $now };
EXPLAIN ANALYZE SELECT * FROM test_user WHERE created_at = $now;
EXPLAIN ANALYZE SELECT * FROM test_user WHERE created_at = d'2023-12-25T17:13:01.940183014Z';
SELECT * FROM test_user WHERE created_at = $now;
SELECT * FROM test_user WHERE created_at = d'2023-12-25T17:13:01.940183014Z';

-- UUID value
DEFINE INDEX sessionUid ON sessions FIELDS sessionUid;
CREATE sessions:1 CONTENT { sessionUid: u'00ad70db-f435-442e-9012-1cd853102084' };
LET $sess = { uuid: u'00ad70db-f435-442e-9012-1cd853102084' };
EXPLAIN ANALYZE SELECT * FROM sessions WHERE sessionUid = u'00ad70db-f435-442e-9012-1cd853102084';
EXPLAIN ANALYZE SELECT * FROM sessions WHERE sessionUid = $sess.uuid;
SELECT * FROM sessions WHERE sessionUid = u'00ad70db-f435-442e-9012-1cd853102084';
SELECT * FROM sessions WHERE sessionUid = $sess.uuid;
