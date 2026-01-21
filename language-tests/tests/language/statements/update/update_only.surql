/**
[test]

[[test.results]]
value = "[{ id: test:a, name: 'A' }]"

[[test.results]]
value = "{ id: test:a, name: 'A' }"

*/

CREATE test:a SET name = 'A';
UPDATE ONLY test:a SET name = 'A';
