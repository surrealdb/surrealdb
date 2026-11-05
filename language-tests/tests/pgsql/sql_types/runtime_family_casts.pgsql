/**
[env]
namespace = true
database = true

[test]
reason = "a cast of a value read at run time to a type with a family of its own reads its text as a write to a column of the type does: a range is parsed and normalised, a network canonicalised and ordered as a network, and text the type cannot read is refused"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ addr: '10.0.0.2/32', id: 1, span: '[1,5]' }, { addr: '9.255.255.255', id: 2, span: '(3,9)' }, { addr: NULL, id: 3, span: NULL }]"

[[test.results]]
value = "[{ hi: 6i32, id: 1, lo: 1i32, r: 1i32..6i32 }, { hi: 9i32, id: 2, lo: 4i32, r: 4i32..9i32 }, { hi: NULL, id: 3, lo: NULL, r: NULL }]"

[[test.results]]
value = "[{ a: '9.255.255.255', id: 2 }, { a: '10.0.0.2', id: 1 }]"

[[test.results]]
value = "[{ id: 1 }, { id: 2 }]"

[[test.results]]
error = 'invalid input syntax for type int4range: "[1,x)" (a range bound is not a value of the element type: x)'
*/

CREATE TABLE raw (id int PRIMARY KEY, span text, addr text);

INSERT INTO raw (id, span, addr) VALUES (1, '[1,5]', '10.0.0.2/32'), (2, '(3,9)', '9.255.255.255'), (3, NULL, NULL);

SELECT id, lower(span::int4range) AS lo, upper(span::int4range) AS hi, span::int4range AS r FROM raw ORDER BY id;

SELECT id, addr::inet AS a FROM raw WHERE addr::inet < '10.0.0.3' ORDER BY addr::inet;

SELECT id FROM raw WHERE span::int4range @> '[4,5)'::int4range ORDER BY id;

SELECT ('[' || id || ',x)')::int4range FROM raw WHERE id = 1;
