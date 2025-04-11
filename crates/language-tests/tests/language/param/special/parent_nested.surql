/**
[test]

[[test.results]]
value = "[[[{ a: 1 }]]]"

[[test.results]]
value = "[[[]]]"

*/
SELECT VALUE (SELECT VALUE (SELECT * FROM [{a: 1}] WHERE a == $parent.value) FROM [{value: 1}] ) FROM [{value: 2}];
SELECT VALUE (SELECT VALUE (SELECT * FROM [{a: 1}] WHERE a == $parent.value) FROM [{value: 3}] ) FROM [{value: 2}];
