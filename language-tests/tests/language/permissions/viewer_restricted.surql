/**
[env]
auth = { level = "owner" }

[test]
reason = "Test permission checks by creating data as owner then querying as viewer"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ data: 'classified', id: secret:1 }]"

[[test.results]]
value = "[{ data: 'open', id: public:1 }]"

[[test.results]]
value = "[{ data: 'classified', id: secret:1 }]"

[[test.results]]
value = "[{ data: 'open', id: public:1 }]"

*/

DEFINE TABLE secret PERMISSIONS FOR select WHERE $auth.role = 'owner', FOR create, update, delete NONE;
DEFINE TABLE public PERMISSIONS FULL;
CREATE secret:1 SET data = 'classified';
CREATE public:1 SET data = 'open';
SELECT * FROM secret;
SELECT * FROM public;
