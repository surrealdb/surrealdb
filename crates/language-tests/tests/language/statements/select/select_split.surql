/**
[test]

[[test.results]]
value = "[{ arr: [1, 2, 3], id: test:a }]"

[[test.results]]
value = "[{ arr: [4, 5, 6], id: test:b }]"

[[test.results]]
value = "[{ arr: 1, id: test:a }, { arr: 2, id: test:a }, { arr: 3, id: test:a }, { arr: 4, id: test:b }, { arr: 5, id: test:b }, { arr: 6, id: test:b }]"

[[test.results]]
value = "[{ arr: 1, id: test:a }, { arr: 2, id: test:a }, { arr: 3, id: test:a }, { arr: 4, id: test:b }, { arr: 5, id: test:b }, { arr: 6, id: test:b }]"

*/
CREATE test:a SET arr = [1, 2, 3];
CREATE test:b SET arr = [4, 5, 6];
SELECT * FROM test SPLIT arr;
SELECT * FROM test SPLIT arr ORDER BY id;