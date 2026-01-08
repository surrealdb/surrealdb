/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: user:john }, { id: user:mary }]"

[[test.results]]
value = "[{ id: friends_with:1, in: user:john, out: user:mary }]"

[[test.results]]
value = "[{ id: friends_with:2, in: user:mary, out: user:john }]"

*/

DEFINE TABLE friends_with PERMISSIONS FOR delete WHERE in = $auth;
CREATE user:john, user:mary;
RELATE user:john->friends_with:1->user:mary;
RELATE user:mary->friends_with:2->user:john;
