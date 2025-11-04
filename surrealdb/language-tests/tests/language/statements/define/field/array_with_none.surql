/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: user:test, test: [NONE, 2, NULL] }]"

*/
DEFINE FIELD test ON user TYPE array<option<number|NULL>>;
CREATE user:test SET test = [NONE, 2, NULL];
