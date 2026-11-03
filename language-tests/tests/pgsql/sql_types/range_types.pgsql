/**
[env]
namespace = true
database = true
imports = ["pgsql/sql_types/_ranges.surql"]

[test]
reason = "range and multirange columns read their PostgreSQL text on write and on a cast from text, a discrete range is canonical, @> and <@ test an element, and a range compares equal to a cast literal"

[[test.results]]
value = "[{ days: pd'2024-01-01'..pd'2024-02-01', id: 1, seats: 1i32..6i32, spans: [1i32..6i32] }, { days: pd'1970-01-01'..pd'1970-01-01', id: 2, seats: 11i32..20i32, spans: [] }]"

[[test.results]]
value = "[{ days: pd'2024-01-01'..pd'2024-02-01', id: 1, seats: 1i32..6i32, spans: [1i32..6i32] }, { days: pd'1970-01-01'..pd'1970-01-01', id: 2, seats: 11i32..20i32, spans: [] }]"

[[test.results]]
value = "[{ id: 1 }]"

[[test.results]]
value = "[{ id: 2 }]"

[[test.results]]
value = "[{ id: 1 }]"

[[test.results]]
value = "[{ e: 'empty', r: '[1,6)', u: '(,8)' }]"
*/

INSERT INTO booking (id, seats, days, spans) VALUES
	(1, '[1,5]', '[2024-01-01,2024-01-31]', '{[1,3),[2,6)}'),
	(2, '(10,20)', 'empty', '{}');

SELECT id, seats, days, spans FROM booking ORDER BY id;

SELECT id FROM booking WHERE seats @> 3;

SELECT id FROM booking WHERE 15 <@ seats;

SELECT id FROM booking WHERE seats = '[1,6)'::int4range;

SELECT '[1,5]'::int4range::text AS r, '(,7]'::int8range::text AS u, 'empty'::int4range::text AS e;
