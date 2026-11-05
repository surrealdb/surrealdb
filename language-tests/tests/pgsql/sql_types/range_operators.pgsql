/**
[env]
namespace = true
database = true
imports = ["pgsql/sql_types/_ranges.surql"]

[test]
reason = "&& tests whether two ranges overlap, @> and <@ between two ranges test containment, and lower() and upper() give a range's bounds, NULL for an empty range or a missing bound"

[[test.results]]
value = "[{ days: pd'2024-01-01'..pd'2024-02-01', id: 1, seats: 1i32..6i32 }, { days: pd'1970-01-01'..pd'1970-01-01', id: 2, seats: 11i32..20i32 }, { days: pd'2024-01-20'.., id: 3, seats: 4i32..12i32 }]"

[[test.results]]
value = "[{ id: 1 }, { id: 3 }]"

[[test.results]]
value = "[{ id: 1 }]"

[[test.results]]
value = "[{ id: 2 }]"

[[test.results]]
value = "[{ id: 1 }, { id: 3 }]"

[[test.results]]
value = "[{ hi: 6i32, id: 1, lo: 1i32 }, { hi: 20i32, id: 2, lo: 11i32 }, { hi: 12i32, id: 3, lo: 4i32 }]"

[[test.results]]
value = "[{ hi: pd'2024-02-01', id: 1, lo: pd'2024-01-01' }, { hi: NULL, id: 2, lo: NULL }, { hi: NULL, id: 3, lo: pd'2024-01-20' }]"

[[test.results]]
value = "[{ sharing: true, touching: false }]"

[[test.results]]
value = "[{ empty_holds: false, holds_empty: true }]"

[[test.results]]
value = "[{ e: NULL, hi: NULL, lo: NULL }]"
*/

INSERT INTO booking (id, seats, days) VALUES
	(1, '[1,5]', '[2024-01-01,2024-01-31]'),
	(2, '(10,20)', 'empty'),
	(3, '[4,12)', '[2024-01-20,)');

SELECT id FROM booking WHERE seats && '[5,11)'::int4range ORDER BY id;

SELECT id FROM booking WHERE seats @> '[2,3)'::int4range ORDER BY id;

SELECT id FROM booking WHERE '[12,15)'::int4range <@ seats ORDER BY id;

SELECT id FROM booking WHERE days && '[2024-01-25,2024-02-01)'::daterange ORDER BY id;

SELECT id, lower(seats) AS lo, upper(seats) AS hi FROM booking ORDER BY id;

SELECT id, lower(days) AS lo, upper(days) AS hi FROM booking ORDER BY id;

SELECT '[1,5)'::int4range && '[5,8)'::int4range AS touching, '[1,5]'::int4range && '[5,8)'::int4range AS sharing;

SELECT '[1,10)'::int4range @> 'empty'::int4range AS holds_empty, 'empty'::int4range @> '[1,2)'::int4range AS empty_holds;

SELECT lower('(,5)'::int4range) AS lo, upper('[3,)'::int4range) AS hi, lower('empty'::int4range) AS e;
