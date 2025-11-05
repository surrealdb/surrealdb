/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = '''{ events: {  }, fields: { test: "DEFINE FIELD test ON user VALUE $value OR 'GBR' PERMISSIONS FULL" }, indexes: {  }, lives: {  }, tables: {  } }'''

[[test.results]]
value = "[{ id: user:a, test: 'GBR' }]"

[[test.results]]
value = "[{ id: user:b, test: 'b' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: user:c, test: 'GBR' }]"

[[test.results]]
value = "[{ id: user:a, test: 'GBR' }, { id: user:b, test: 'b' }, { id: user:c, test: 'GBR' }]"

*/
DEFINE FIELD test ON user VALUE $value OR 'GBR';
REMOVE FIELD test ON user;
DEFINE FIELD test ON TABLE user VALUE $value OR 'GBR';
INFO FOR TABLE user;

CREATE user:a;
CREATE user:b SET test = "b";
let $value = "NL";
CREATE user:c;

SELECT * from user;
