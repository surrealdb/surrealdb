/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ custom: ['sometext'], id: user:one }]"

[[test.results]]
value = "[{ custom: [['sometext']], id: user:two }]"

[[test.results]]
value = "[{ custom: [{ key: 'sometext' }], id: user:three }]"

*/

DEFINE TABLE user SCHEMAFULL;
DEFINE FIELD custom ON user TYPE option<array>;
DEFINE FIELD custom.* ON user FLEXIBLE TYPE any;
CREATE user:one CONTENT { custom: ['sometext'] };
CREATE user:two CONTENT { custom: [ ['sometext'] ] };
CREATE user:three CONTENT { custom: [ { key: 'sometext' } ] };
