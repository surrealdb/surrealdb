/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "Relation query with WHERE clause referencing $parent does not resolve correctly"
issue = 7083

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: user:1 }]"

[[test.results]]
value = "[{ id: user:2 }]"

[[test.results]]
value = "[{ id: follows_user:x, in: user:1, out: user:2 }]"
skip-record-id-key = true

[[test.results]]
value = "[{ id: follows_user:x, in: user:2, out: user:1 }]"
skip-record-id-key = true

[[test.results]]
value = "[{ followers: [{ in: user:1, out: user:2 }], in: user:2, isFollowing: true, out: user:1 }]"

*/

DEFINE TABLE OVERWRITE user SCHEMAFULL;

DEFINE TABLE OVERWRITE follows_user SCHEMAFULL TYPE RELATION FROM user TO user ENFORCED;
DEFINE FIELD OVERWRITE in ON follows_user TYPE record<user> ASSERT in != out;
DEFINE FIELD OVERWRITE out ON follows_user TYPE record<user> ASSERT out != in;

CREATE user:1;
CREATE user:2;

RELATE user:1->follows_user->user:2;
RELATE user:2->follows_user->user:1;

-- Core reproduction: $parent.in in graph WHERE should reference the outer row
SELECT
  in,
  out,
  !!(user:1->follows_user[WHERE out=$parent.in]) as isFollowing,
  user:1->follows_user.{in, out} as followers
FROM user:1<-follows_user;
