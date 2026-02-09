/**
[test]

[[test.results]]
value = "[{ id: t:1, tags: {1, 2, 3} }]"

[[test.results]]
value = "[{ id: t:1, tags: 1 }, { id: t:1, tags: 2 }, { id: t:1, tags: 3 }]"

*/
CREATE t:1 SET tags = {1, 2, 3};
SELECT * FROM t SPLIT tags;
