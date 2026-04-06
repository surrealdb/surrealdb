/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "$parent in a graph WHERE inside a nested subquery should refer to the inner SELECT's row"
issue = 7184

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
value = "[{ back: [{ followers: [{ in: user:2, out: user:1 }], in: user:1, isFollowing: true, out: user:2 }], followers: [{ in: user:1, out: user:2 }], in: user:2, isFollowing: true, out: user:1 }]"

*/

DEFINE TABLE OVERWRITE user SCHEMAFULL;

DEFINE TABLE OVERWRITE follows_user SCHEMAFULL TYPE RELATION FROM user TO user ENFORCED;
DEFINE FIELD OVERWRITE in ON follows_user TYPE record<user> ASSERT in != out;
DEFINE FIELD OVERWRITE out ON follows_user TYPE record<user> ASSERT out != in;

CREATE user:1;
CREATE user:2;

RELATE user:1->follows_user->user:2;
RELATE user:2->follows_user->user:1;

-- Outer isFollowing uses $parent = outer row, inner isFollowing uses $parent = inner row.
-- Both should be true.
SELECT
  in,
  out,
  !!(user:1->follows_user[WHERE out=$parent.in]) as isFollowing,
  user:1->follows_user.{in, out} as followers,
  (SELECT
    in,
    out,
    !!(user:2->follows_user[WHERE out=$parent.in]) as isFollowing,
    user:2->follows_user.{in, out} as followers
    FROM user:2<-follows_user
  ) as back
FROM user:1<-follows_user;
