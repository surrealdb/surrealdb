/**
[test]
reason = "A step keeps the test it was minted with: an ASSERT added afterwards does not change what an old row becomes."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: '5', id: tb:1 }]"

[[test.results]]
value = "[{ a: 'x', id: tb:2 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[5, 0]"
*/
DEFINE TABLE tb SCHEMAFULL;
DEFINE FIELD a ON tb TYPE string;
CREATE tb:1 SET a = '5';
CREATE tb:2 SET a = 'x';
ALTER FIELD a ON tb TYPE int MIGRATE EXISTING ROWS AS a_int USING <int>$value FALLBACK 0;
ALTER FIELD a ON tb ASSERT $value > 3;
SELECT VALUE a FROM tb ORDER BY id;
