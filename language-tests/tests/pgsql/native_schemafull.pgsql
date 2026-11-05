/**
[env]
namespace = true
database = true
imports = ["pgsql/_native_schemafull.surql"]

[test]
reason = "PostgreSQL reads over schemafull tables: * is the declared columns, an optional one a row lacks reading as NONE, defaults, VALUE and computed fields on the rows of each side of a join, an aggregate and a set operation"

[[test.results]]
value = "[{ acct_no: 1, balance: 50, double: 100, id: 1, name: 'Ann', note: 'vip', tier: 'gold' }, { acct_no: 2, balance: 0, double: 0, id: 2, name: 'Ben', note: NONE, tier: 'basic' }, { acct_no: 3, balance: 12.5f, double: 25f, id: 3, name: 'Cy', note: NONE, tier: 'silver' }, { acct_no: 4, balance: 200, double: 400, id: 4, name: 'Di', note: NONE, tier: 'basic' }]"

[[test.results]]
value = "[{ double: 100, name: 'Ann', tier: 'gold' }, { double: 0, name: 'Ben', tier: 'basic' }, { double: 25f, name: 'Cy', tier: 'silver' }, { double: 400, name: 'Di', tier: 'basic' }]"

[[test.results]]
value = "[{ name: 'Ben', note: NONE }, { name: 'Cy', note: NONE }, { name: 'Di', note: NONE }]"

[[test.results]]
value = "[{ amount: 10, kind: 'credit', name: 'Ann' }, { amount: 30, kind: 'debit', name: 'Ann' }, { amount: 5, kind: 'credit', name: 'Cy' }]"

[[test.results]]
value = "[{ amount: 10, double: 100, name: 'Ann' }, { amount: 30, double: 100, name: 'Ann' }, { amount: NONE, double: 0, name: 'Ben' }, { amount: 5, double: 25f, name: 'Cy' }, { amount: NONE, double: 400, name: 'Di' }]"

[[test.results]]
value = "[{ amount: 10, name: 'Ann' }, { amount: 30, name: 'Ann' }]"

[[test.results]]
value = "[{ n: 2, tier: 'basic', total: 200 }, { n: 1, tier: 'gold', total: 50 }, { n: 1, tier: 'silver', total: 12.5f }]"

[[test.results]]
value = "[{ moved: 40, name: 'Ann' }, { moved: 5, name: 'Cy' }]"

[[test.results]]
value = "[{ moved: 40, name: 'Ann' }]"

[[test.results]]
value = "[{ kind: 'credit', n: 3 }, { kind: 'debit', n: 1 }]"

[[test.results]]
value = "[{ tier: 'basic' }, { tier: 'gold' }, { tier: 'silver' }]"

[[test.results]]
value = "[{ n: 'Ann' }, { n: 'Ben' }, { n: 'Cy' }, { n: 'Di' }, { n: 'credit' }, { n: 'debit' }]"

[[test.results]]
value = "[{ name: 'Di' }]"

[[test.results]]
value = "[{ name: 'Di' }, { name: 'Ann' }, { name: 'Cy' }, { name: 'Ben' }]"

[[test.results]]
value = "[{ double: 100, name: 'Ann' }, { double: 25f, name: 'Cy' }, { double: 400, name: 'Di' }]"
*/

SELECT * FROM acct a ORDER BY a.acct_no;

SELECT a.name, a.tier, a.double FROM acct a ORDER BY a.name;

SELECT a.name, a.note FROM acct a WHERE a.note IS NULL ORDER BY a.name;

SELECT a.name, t.amount, t.kind FROM acct a JOIN tx t ON t.acct_id = a.acct_no ORDER BY a.name, t.amount;

SELECT a.name, a.double, t.amount FROM acct a LEFT JOIN tx t ON t.acct_id = a.acct_no ORDER BY a.name, t.amount;

SELECT a.name, t.amount FROM tx t JOIN acct a ON t.acct_id = a.acct_no WHERE a.tier = 'gold' ORDER BY t.amount;

SELECT a.tier, count(*) AS n, sum(a.balance) AS total FROM acct a GROUP BY a.tier ORDER BY a.tier;

SELECT a.name, sum(t.amount) AS moved FROM acct a JOIN tx t ON t.acct_id = a.acct_no GROUP BY a.name ORDER BY a.name;

SELECT a.name, sum(t.amount) AS moved FROM acct a LEFT JOIN tx t ON t.acct_id = a.acct_no GROUP BY a.name HAVING sum(t.amount) > 5 ORDER BY a.name;

SELECT t.kind, count(*) AS n FROM tx t GROUP BY t.kind ORDER BY t.kind;

SELECT DISTINCT a.tier FROM acct a ORDER BY a.tier;

SELECT a.name AS n FROM acct a UNION SELECT t.kind AS n FROM tx t ORDER BY n;

SELECT a.name FROM acct a WHERE a.double > 100 ORDER BY a.name;

SELECT a.name FROM acct a ORDER BY a.double DESC, a.name;

SELECT s.name, s.double FROM (SELECT a.name AS name, a.double AS double FROM acct a) s WHERE s.double > 20 ORDER BY s.name;
