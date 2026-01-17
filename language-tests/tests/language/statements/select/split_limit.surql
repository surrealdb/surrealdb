/**
[test]

[[test.results]]
value = "[{ a: [1], id: t:a }]"

[[test.results]]
value = "[{ a: 1, id: t:a }]"

[[test.results]]
value = "[{ a: [2, 3, 4], id: t:b }]"

[[test.results]]
value = "[{ a: 1, id: t:a }, { a: 2, id: t:b }, { a: 3, id: t:b }, { a: 4, id: t:b }]"

[[test.results]]
value = "[{ a: 1, id: t:a }, { a: 2, id: t:b }, { a: 3, id: t:b }, { a: 4, id: t:b }]"

[[test.results]]
value = "[{ a: 1, id: t:a }, { a: 2, id: t:b }, { a: 3, id: t:b }, { a: 4, id: t:b }]"

[[test.results]]
value = "[{ a: 2, id: t:b }, { a: 3, id: t:b }]"

[[test.results]]
value = "[{ a: 2, id: t:b }, { a: 3, id: t:b }]"

[[test.results]]
value = "[{ a: 2, id: t:b }, { a: 3, id: t:b }]"

*/
CREATE t:a SET a = [1];
SELECT * FROM t SPLIT a ORDER BY rand();
CREATE t:b SET a = [2, 3, 4];
SELECT * FROM t SPLIT a;
SELECT * FROM t SPLIT a ORDER BY id;
SELECT * FROM t SPLIT a ORDER BY id TEMPFILES;
SELECT * FROM t SPLIT a ORDER BY id START 1 LIMIT 2;
SELECT * FROM t SPLIT a START 1 LIMIT 2;
SELECT * FROM t SPLIT a ORDER BY id START 1 LIMIT 2 TEMPFILES;
