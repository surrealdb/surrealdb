/**
[env]
namespace = true
database = true
imports = ["pgsql/sql_types/_ranges.surql"]

[test]
reason = "an untyped literal opposite a range in &&, @> or <@ is read as a range of the same type, as PostgreSQL infers it"

[[test.results]]
value = "[{ days: pd'2024-01-01'..pd'2024-02-01', id: 1, seats: 1i32..6i32 }, { days: pd'1970-01-01'..pd'1970-01-01', id: 2, seats: 11i32..20i32 }]"

[[test.results]]
value = "[{ id: 1 }]"

[[test.results]]
value = "[{ id: 1 }]"

[[test.results]]
value = "[{ id: 1 }]"

[[test.results]]
value = "[{ id: 1 }, { id: 2 }]"

[[test.results]]
value = "[{ id: 2 }]"
*/

INSERT INTO booking (id, seats, days) VALUES (1, '[1,5]', '[2024-01-01,2024-01-31]'), (2, '(10,20)', 'empty');

SELECT id FROM booking WHERE seats && '[5,11)' ORDER BY id;

SELECT id FROM booking WHERE '[2024-01-25,2024-02-01)' && days ORDER BY id;

SELECT id FROM booking WHERE seats @> '[2,4)' ORDER BY id;

SELECT id FROM booking WHERE seats <@ '[0,30)' ORDER BY id;

SELECT id FROM booking WHERE '[12,15)' <@ seats ORDER BY id;
