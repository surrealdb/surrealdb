/**
[test]
reason = "Test SELECT FROM subquery with basic field selections"

[[test.results]]
value = "[{ id: test:1, name: 'One', number: 10 }]"

[[test.results]]
value = "[{ id: test:2, name: 'Two', number: 20 }]"

[[test.results]]
value = "[{ id: test:3, name: 'Three', number: 30 }]"

[[test.results]]
value = "[{ id: test:1, name: 'One', number: 10 }, { id: test:2, name: 'Two', number: 20 }, { id: test:3, name: 'Three', number: 30 }]"

[[test.results]]
value = "[{ id: test:1 }, { id: test:2 }, { id: test:3 }]"

[[test.results]]
value = "[{ id: test:1, number: 10 }, { id: test:2, number: 20 }, { id: test:3, number: 30 }]"

[[test.results]]
value = "[{ id: other:1, name: 'One', number: 10 }, { id: other:2, name: 'Two', number: 20 }, { id: other:3, name: 'Three', number: 30 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: other:1 }, { id: other:2 }, { id: other:3 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: other:1, number: 10 }, { id: other:2, number: 20 }, { id: other:3, number: 30 }]"

*/

CREATE test:1 SET number = 10, name = 'One';

CREATE test:2 SET number = 20, name = 'Two';

CREATE test:3 SET number = 30, name = 'Three';

SELECT * FROM (SELECT VALUE id FROM test);

SELECT * FROM (SELECT id FROM test);

SELECT * FROM (SELECT id, number FROM test);

INSERT INTO other (SELECT * FROM test);

DELETE other;

INSERT INTO other (SELECT id FROM test);

DELETE other;

INSERT INTO other (SELECT id, number FROM test);