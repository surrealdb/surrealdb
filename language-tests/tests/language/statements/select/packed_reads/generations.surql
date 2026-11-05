/**
[env]
namespace = true
database = true
compatibility = 3

[test]
reason = "Rows of one SCHEMAFULL table written under several layouts, and rows written before the table was SCHEMAFULL as documents, read alike: every read answers what the whole decode of each row holds."

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ age: 41, extra: [1, 2], id: person:1, legacy: 'old', name: 'ann' }]"

[[test.results]]
value = "[{ age: 37, id: person:2, name: 'bob' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ age: 29, id: person:3, legacy: 'older', name: 'cy' }]"

[[test.results]]
value = "[{ age: 52, id: person:4, name: 'di' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ age: 61, email: 'ed@example.com', id: person:5, name: 'ed' }]"

[[test.results]]
value = "[{ age: 18, email: 'flo@example.com', id: person:6, legacy: 'x', name: 'flo' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ age: 44, id: person:7, name: 'gus' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ age: 'forty', id: person:8, name: 'hal' }]"

[[test.results]]
value = "[{ age: 33, id: person:9, name: 'ivy' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ age: 26, id: person:10, name: 'jo', score: 9.5f }]"

[[test.results]]
value = "[{ age: 41, extra: [1, 2], id: person:1, legacy: 'old', name: 'ann' }, { age: 37, id: person:2, name: 'bob' }, { age: 29, id: person:3, legacy: 'older', name: 'cy' }, { age: 52, id: person:4, name: 'di' }, { age: 61, email: 'ed@example.com', id: person:5, name: 'ed' }, { age: 18, email: 'flo@example.com', id: person:6, legacy: 'x', name: 'flo' }, { age: 44, id: person:7, name: 'gus' }, { age: 'forty', id: person:8, name: 'hal' }, { age: 33, id: person:9, name: 'ivy' }, { age: 26, id: person:10, name: 'jo', score: 9.5f }]"

[[test.results]]
value = "[{ count: 10 }]"

[[test.results]]
value = "[person:1, person:2, person:4, person:5, person:7, person:8, person:9]"

[[test.results]]
value = "[person:8]"

[[test.results]]
value = "[person:7]"

[[test.results]]
value = "[person:3]"

[[test.results]]
value = "[person:1, person:5, person:10]"

[[test.results]]
value = "[person:1, person:2, person:3, person:4, person:7, person:8, person:9, person:10]"

[[test.results]]
value = "[person:5, person:6]"

[[test.results]]
value = "[person:5, person:6]"

[[test.results]]
value = "[person:1, person:2, person:3, person:4, person:5, person:6, person:7, person:8, person:9]"

[[test.results]]
value = "[person:10]"

[[test.results]]
value = "[person:2, person:3, person:4, person:5, person:6, person:7, person:8, person:9, person:10]"

[[test.results]]
value = "[person:5]"

[[test.results]]
value = "[41]"

[[test.results]]
value = "['ivy']"

[[test.results]]
value = "[{ age: 41, name: 'ann' }, { age: 52, name: 'di' }, { age: 61, name: 'ed' }, { age: 44, name: 'gus' }, { age: 'forty', name: 'hal' }]"

[[test.results]]
value = "[{ email: NONE, name: 'ann' }, { email: NONE, name: 'bob' }, { email: NONE, name: 'cy' }, { email: NONE, name: 'di' }, { email: 'ed@example.com', name: 'ed' }, { email: 'flo@example.com', name: 'flo' }, { email: NONE, name: 'gus' }, { email: NONE, name: 'hal' }, { email: NONE, name: 'ivy' }, { email: NONE, name: 'jo' }]"

[[test.results]]
value = "[person:10, person:9, person:8, person:7, person:6, person:5, person:4, person:3, person:2, person:1]"

[[test.results]]
value = "[person:6, person:10, person:3, person:9, person:2, person:1, person:7, person:4, person:5, person:8]"

[[test.results]]
value = "[person:1, person:2, person:3, person:4, person:7, person:8, person:9, person:10, person:5, person:6]"

[[test.results]]
value = "[person:10, person:1, person:2, person:3]"

[[test.results]]
value = "[{ count: 8 }]"

[[test.results]]
value = "[{ count: 7 }]"

[[test.results]]
value = "[{ total: 341 }]"

[[test.results]]
value = "[{ age: 45, id: person:7, name: 'gus' }]"

[[test.results]]
value = "[{ age: 37, email: 'bob@example.com', id: person:2, name: 'bob' }]"

[[test.results]]
value = "[person:7]"

[[test.results]]
value = "[{ age: 37, email: 'bob@example.com', id: person:2, name: 'bob' }, { age: 61, email: 'ed@example.com', id: person:5, name: 'ed' }, { age: 18, email: 'flo@example.com', id: person:6, legacy: 'x', name: 'flo' }]"

[[test.results]]
value = "[{ age: 37, email: 'bob@example.com', id: person:2, name: 'bob' }]"
*/

DEFINE TABLE person SCHEMALESS;
CREATE person:1 CONTENT { name: 'ann', age: 41, legacy: 'old', extra: [1, 2] };
CREATE person:2 CONTENT { name: 'bob', age: 37 };
DEFINE TABLE OVERWRITE person SCHEMAFULL;
DEFINE FIELD name ON person TYPE string;
DEFINE FIELD age ON person TYPE int;
DEFINE FIELD legacy ON person TYPE option<string>;
CREATE person:3 CONTENT { name: 'cy', age: 29, legacy: 'older' };
CREATE person:4 CONTENT { name: 'di', age: 52 };
DEFINE FIELD email ON person TYPE option<string>;
CREATE person:5 CONTENT { name: 'ed', age: 61, email: 'ed@example.com' };
CREATE person:6 CONTENT { name: 'flo', age: 18, legacy: 'x', email: 'flo@example.com' };
REMOVE FIELD legacy ON person;
CREATE person:7 CONTENT { name: 'gus', age: 44 };
DEFINE FIELD OVERWRITE age ON person TYPE int | string;
CREATE person:8 CONTENT { name: 'hal', age: 'forty' };
CREATE person:9 CONTENT { name: 'ivy', age: 33, email: NONE };
DEFINE FIELD score ON person TYPE option<float>;
CREATE person:10 CONTENT { name: 'jo', age: 26, score: 9.5 };

SELECT * FROM person;
SELECT count() FROM person GROUP ALL;
SELECT VALUE id FROM person WHERE age > 30;
SELECT VALUE id FROM person WHERE age = 'forty';
SELECT VALUE id FROM person WHERE age = 44;
SELECT VALUE id FROM person WHERE name = 'cy';
SELECT VALUE id FROM person WHERE name IN ['ann', 'ed', 'jo'];
SELECT VALUE id FROM person WHERE email IS NONE;
SELECT VALUE id FROM person WHERE email IS NOT NONE;
SELECT VALUE id FROM person WHERE email CONTAINS 'example';
SELECT VALUE id FROM person WHERE score IS NONE;
SELECT VALUE id FROM person WHERE score > 9;
SELECT VALUE id FROM person WHERE extra IS NONE;
SELECT VALUE id FROM person WHERE age > 30 AND email IS NOT NONE;
SELECT VALUE age FROM person WHERE name = 'ann';
SELECT VALUE name FROM person WHERE age = 33;
SELECT name, age FROM person WHERE age > 40;
SELECT name, email FROM person;
SELECT VALUE id FROM person ORDER BY name DESC;
SELECT VALUE id FROM person ORDER BY age, id;
SELECT VALUE id FROM person ORDER BY email, id;
SELECT VALUE id FROM person ORDER BY score DESC, id LIMIT 4;
SELECT count() FROM person WHERE email IS NONE GROUP ALL;
SELECT count() FROM person WHERE age > 30 GROUP ALL;
SELECT math::sum(age) AS total FROM person WHERE type::is_int(age) GROUP ALL;
UPDATE person:7 SET age = 45 RETURN AFTER;
UPDATE person:2 SET email = 'bob@example.com' RETURN AFTER;
SELECT VALUE id FROM person WHERE age = 45;
SELECT * FROM person WHERE email IS NOT NONE;
SELECT * FROM person WHERE name = 'bob';
