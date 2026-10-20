/**
[env]
namespace = true
database = true

[test]
reason = "A record whose nested record field is NONE reads back as NONE and does not fail as a record missing its fields. The report was a client struct that required the field to be present; the server result is pinned here."
issue = 5524

[[test.results]]
value = "[{ id: test:a, name: 'a' }]"

[[test.results]]
value = "[{ forked: [test:a], id: ut:1 }]"

[[test.results]]
value = "[{ forked: ['a'], test: NONE }]"
*/
CREATE test:a SET name = 'a';
CREATE ut:1 SET test = NONE, forked = [test:a];
SELECT test, forked.name AS forked FROM ut:1;
