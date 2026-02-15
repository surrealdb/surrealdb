/**
[test]
reason = "Test IN and INSIDE operators with standard, unique, and compound indexes (new executor)"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ email: 'a@b', id: user:1 }]"

[[test.results]]
value = "[{ email: 'c@d', id: user:2 }]"

[[test.results]]
value = '''"Project [ctx: Db] {rows: 1}
└────> TableScan [ctx: Db] [table: user, direction: Forward, predicate: email INSIDE ['a@b', 'e@f']] {rows: 1}

Total rows: 1"'''

[[test.results]]
value = '''"Project [ctx: Db] {rows: 1}
└────> TableScan [ctx: Db] [table: user, direction: Forward, predicate: email INSIDE ['a@b', 'e@f']] {rows: 1}

Total rows: 1"'''

[[test.results]]
value = "[{ email: 'a@b', id: user:1 }]"

[[test.results]]
value = "[{ email: 'a@b', id: user:1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ apprenantUid: '00013483-fedd-43e3-a94e-80728d896f6e', id: apprenants:1 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ apprenantUid: '00013483-fedd-43e3-a94e-80728d896f6e' }]"

[[test.results]]
value = "[{ apprenantUid: '00013483-fedd-43e3-a94e-80728d896f6e' }]"

[[test.results]]
value = "[{ apprenantUid: '00013483-fedd-43e3-a94e-80728d896f6e' }]"

[[test.results]]
value = "[{ apprenantUid: '00013483-fedd-43e3-a94e-80728d896f6e' }]"

[[test.results]]
value = '''"Project [ctx: Db] {rows: 1}
└────> TableScan [ctx: Db] [table: apprenants, direction: Forward, predicate: apprenantUid INSIDE ['00013483-fedd-43e3-a94e-80728d896f6e']] {rows: 1}

Total rows: 1"'''

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ account: accounts:1, id: test:1, type: 'password', value: '$argon2id$v=19$m=19456,t=2,p=1$qP0Y6ApAaFyeDsGySVD0DQ$Bp/Jv2uwapFUl5NKYk3uMsT5DZV0XPpMMVm3F2ZVfSM' }]"

[[test.results]]
value = "[{ account: accounts:2, id: test:2, type: 'password', value: '$argon2id$v=19$m=19456,t=2,p=1$nwwBkmNrUjjfD1ny3W50Ow$ZyZcp9GaH5iQx3k8YUmR/R3nguJR9feFO1cewEGGjcM' }]"

[[test.results]]
value = "[{ account: accounts:3, id: test:3, type: 'password', value: 'notAValidPassword' }]"

[[test.results]]
value = '''"Project [ctx: Db] {rows: 1}
└────> TableScan [ctx: Db] [table: test, direction: Forward, predicate: account = accounts:1 AND type INSIDE ['password', 'firebasePassword']] {rows: 1}

Total rows: 1"'''

[[test.results]]
value = '''"Project [ctx: Db] {rows: 1}
└────> Filter [ctx: Db] [predicate: account = accounts:1 AND type INSIDE ['password', 'firebasePassword']] {rows: 1}
       └────> IndexScan [ctx: Db] [index: idx_account_type, access: [accounts:1], direction: Forward] {rows: 1}

Total rows: 1"'''

[[test.results]]
value = '''"Project [ctx: Db] {rows: 1}
└────> Filter [ctx: Db] [predicate: account = accounts:1 AND type INSIDE ['password', 'firebasePassword']] {rows: 1}
       └────> IndexScan [ctx: Db] [index: idx_account_type, access: [accounts:1], direction: Forward] {rows: 1}

Total rows: 1"'''

[[test.results]]
value = "[{ account: accounts:1, id: test:1, type: 'password', value: '$argon2id$v=19$m=19456,t=2,p=1$qP0Y6ApAaFyeDsGySVD0DQ$Bp/Jv2uwapFUl5NKYk3uMsT5DZV0XPpMMVm3F2ZVfSM' }]"

[[test.results]]
value = "[{ account: accounts:1, id: test:1, type: 'password', value: '$argon2id$v=19$m=19456,t=2,p=1$qP0Y6ApAaFyeDsGySVD0DQ$Bp/Jv2uwapFUl5NKYk3uMsT5DZV0XPpMMVm3F2ZVfSM' }]"

[[test.results]]
value = "[{ account: accounts:1, id: test:1, type: 'password', value: '$argon2id$v=19$m=19456,t=2,p=1$qP0Y6ApAaFyeDsGySVD0DQ$Bp/Jv2uwapFUl5NKYk3uMsT5DZV0XPpMMVm3F2ZVfSM' }]"

[[test.results]]
value = "[{ account: accounts:1, id: test:1, type: 'password', value: '$argon2id$v=19$m=19456,t=2,p=1$qP0Y6ApAaFyeDsGySVD0DQ$Bp/Jv2uwapFUl5NKYk3uMsT5DZV0XPpMMVm3F2ZVfSM' }]"

*/

-- Standard index with IN/INSIDE
DEFINE INDEX user_email_idx ON user FIELDS email;
CREATE user:1 CONTENT { email: 'a@b' };
CREATE user:2 CONTENT { email: 'c@d' };
EXPLAIN ANALYZE SELECT * FROM user WHERE email IN ['a@b', 'e@f'];
EXPLAIN ANALYZE SELECT * FROM user WHERE email INSIDE ['a@b', 'e@f'];
SELECT * FROM user WHERE email IN ['a@b', 'e@f'];
SELECT * FROM user WHERE email INSIDE ['a@b', 'e@f'];

-- Unique index with IN
DEFINE INDEX apprenantUid ON apprenants FIELDS apprenantUid UNIQUE;
CREATE apprenants:1 CONTENT { apprenantUid: "00013483-fedd-43e3-a94e-80728d896f6e" };
SELECT apprenantUid FROM apprenants WHERE apprenantUid in [];

SELECT apprenantUid FROM apprenants WHERE apprenantUid IN ["00013483-fedd-43e3-a94e-80728d896f6e"];
SELECT apprenantUid FROM apprenants WHERE apprenantUid IN ["99999999-aaaa-1111-8888-abcdef012345", "00013483-fedd-43e3-a94e-80728d896f6e"];
SELECT apprenantUid FROM apprenants WHERE apprenantUid IN ["00013483-fedd-43e3-a94e-80728d896f6e", "99999999-aaaa-1111-8888-abcdef012345"];
SELECT apprenantUid FROM apprenants WHERE apprenantUid IN ["99999999-aaaa-1111-8888-abcdef012345", "00013483-fedd-43e3-a94e-80728d896f6e", "99999999-aaaa-1111-8888-abcdef012345"];
EXPLAIN ANALYZE SELECT apprenantUid FROM apprenants WHERE apprenantUid IN ["00013483-fedd-43e3-a94e-80728d896f6e"];

-- Compound index with IN and cross-index selection
DEFINE INDEX idx_account_type ON test FIELDS account, type;
DEFINE INDEX idx_type_value ON test FIELDS type, value;
CREATE test:1 CONTENT {
	account: accounts:1,
	type: 'password',
	value: '$argon2id$v=19$m=19456,t=2,p=1$qP0Y6ApAaFyeDsGySVD0DQ$Bp/Jv2uwapFUl5NKYk3uMsT5DZV0XPpMMVm3F2ZVfSM'
};
CREATE test:2 CONTENT {
	account: accounts:2,
	type: 'password',
	value: '$argon2id$v=19$m=19456,t=2,p=1$nwwBkmNrUjjfD1ny3W50Ow$ZyZcp9GaH5iQx3k8YUmR/R3nguJR9feFO1cewEGGjcM'
};
CREATE test:3 CONTENT {
	account: accounts:3,
	type: 'password',
	value: 'notAValidPassword'
};
EXPLAIN ANALYZE SELECT * FROM test WITH INDEX idx_type_value WHERE account = accounts:1 AND type IN ['password', 'firebasePassword'];
EXPLAIN ANALYZE SELECT * FROM test WITH INDEX idx_account_type WHERE account = accounts:1 AND type IN ['password', 'firebasePassword'];
EXPLAIN ANALYZE SELECT * FROM test WHERE account = accounts:1 AND type IN ['password', 'firebasePassword'];
SELECT * FROM test WITH INDEX idx_type_value WHERE account = accounts:1 AND type IN ['password', 'firebasePassword'];
SELECT * FROM test WITH INDEX idx_account_type WHERE account = accounts:1 AND type IN ['password', 'firebasePassword'];
SELECT * FROM test WHERE account = accounts:1 AND type IN ['password', 'firebasePassword'];

SELECT * FROM test WITH NOINDEX WHERE account = accounts:1 AND type IN ['password', 'firebasePassword'];
