/**
[test]
wip = true

[[test.results]]
value = "NONE"

[[test.results]]
value = '''{ accesses: {  }, namespaces: {  }, nodes: { "6573ee37-3493-449e-9818-4ce20d7aef51": 'NODE 6573ee37-3493-449e-9818-4ce20d7aef51 SEEN 1732807078950 ACTIVE' }, users: { test: "DEFINE USER test ON ROOT PASSHASH '$argon2id$v=19$m=19456,t=2,p=1$QQTQVQZ2Rqp0570585xqDg$KTbTnJxUjslBWhbLJaitCptFBai0ftYqtWyPKXZ6j0w' ROLES VIEWER DURATION FOR TOKEN 1h, FOR SESSION NONE" } }'''

*/

DEFINE USER test ON ROOT PASSWORD 'test';
INFO FOR ROOT;
