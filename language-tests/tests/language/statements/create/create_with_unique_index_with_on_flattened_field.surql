/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ account: 'Apple', emails: ['a@example.com', 'b@example.com'], id: user:1, tags: ['one', 'two'] }]"

[[test.results]]
error = "Database index `test` already contains ['Apple', 'two', ['a@example.com', 'b@example.com']], with record `user:1`"

*/

DEFINE INDEX test ON user FIELDS account, tags, emails... UNIQUE;
CREATE user:1 SET account = 'Apple', tags = ['one', 'two'], emails = ['a@example.com', 'b@example.com'];
CREATE user:2 SET account = 'Apple', tags = ['two', 'three'], emails = ['a@example.com', 'b@example.com'];
