/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ email: 'a@b.com', id: acct:1, ssn: '111-22-3333' }]"

*/
DEFINE TABLE acct SCHEMAFULL;
DEFINE FIELD email ON acct TYPE string PERMISSIONS FULL;
DEFINE FIELD ssn ON acct TYPE option<string> PERMISSIONS FOR select WHERE $auth.admin = true, FOR update NONE;
CREATE acct:1 SET email = 'a@b.com', ssn = '111-22-3333';
