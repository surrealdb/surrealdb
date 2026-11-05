/**
[env]
namespace = true
database = true

[test]
reason = "money, pg_lsn, bit and varbit columns read their PostgreSQL text on write and on a cast from text, and a cast back to text prints the PostgreSQL form"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ amount: 1234.56dec, flags: '1010', id: 1, mask: '101', pos: 97500059720u64 }]"

[[test.results]]
value = "[{ amount: 1234.56dec, flags: '1010', id: 1, mask: '101', pos: 97500059720u64 }]"

[[test.results]]
value = "[{ amount: 1234.50dec, pos: 97500059720u64 }]"

[[test.results]]
value = "[{ amount: '$1,234.50', pos: '16/B374D848' }]"

[[test.results]]
error = "bit string length 3 does not match type bit(4) (field `flags` of `ledger:2`, SQL TYPE bit(4))"
*/

CREATE TABLE ledger (id int PRIMARY KEY, amount money, pos pg_lsn, flags bit(4), mask varbit(8));

INSERT INTO ledger (id, amount, pos, flags, mask) VALUES (1, '$1,234.56', '16/B374D848', '1010', '101');

SELECT id, amount, pos, flags, mask FROM ledger;

SELECT '$1,234.5'::money AS amount, '16/B374D848'::pg_lsn AS pos;

SELECT '1234.5'::money::text AS amount, '16/b374d848'::pg_lsn::text AS pos;

INSERT INTO ledger (id, flags) VALUES (2, '101');
