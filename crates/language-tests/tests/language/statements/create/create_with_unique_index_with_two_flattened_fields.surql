/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ account: 'Apple', emails: ['a@example.com', 'b@example.com'], id: user:1, tags: ['one', 'two'] }]"

[[test.results]]
value = "[{ account: 'Apple', emails: ['a@example.com', 'b@example.com'], id: user:2, tags: ['two', 'three'] }]"

[[test.results]]
error = "Database index `test` already contains ['Apple', ['one', 'two'], ['a@example.com', 'b@example.com']], with record `user:1`"

[[test.results]]
error = "Database index `test` already contains ['Apple', ['two', 'three'], ['a@example.com', 'b@example.com']], with record `user:2`"

*/

DEFINE INDEX test ON user FIELDS account, tags…, emails... UNIQUE;
CREATE user:1 SET account = 'Apple', tags = ['one', 'two'], emails = ['a@example.com', 'b@example.com'];
CREATE user:2 SET account = 'Apple', tags = ['two', 'three'], emails = ['a@example.com', 'b@example.com'];
CREATE user:3 SET account = 'Apple', tags = ['one', 'two'], emails = ['a@example.com', 'b@example.com'];
CREATE user:4 SET account = 'Apple', tags = ['two', 'three'], emails = ['a@example.com', 'b@example.com'];
