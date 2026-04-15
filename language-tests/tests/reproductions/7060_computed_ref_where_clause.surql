/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "COMPUTED reference fields must not return NONE when a WHERE clause is used on an indexed field"
issue = 7060

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: account:1, owner: user:1, profiles: [] }]"

[[test.results]]
value = "[{ account: account:1, id: profile:1 }]"

[[test.results]]
value = "[{ owner: user:1, profiles: [profile:1] }]"

[[test.results]]
value = "[{ owner: user:1, profiles: [profile:1] }]"
*/

DEFINE TABLE OVERWRITE account SCHEMAFULL PERMISSIONS NONE;
DEFINE FIELD OVERWRITE profiles ON TABLE account COMPUTED <~(profile FIELD account);

DEFINE FIELD OVERWRITE owner ON TABLE account TYPE record<user>;
DEFINE INDEX OVERWRITE owner ON TABLE account FIELDS owner;

DEFINE TABLE OVERWRITE profile SCHEMAFULL PERMISSIONS NONE;
DEFINE FIELD OVERWRITE account ON TABLE profile TYPE record<account> REFERENCE ON DELETE REJECT;

CREATE account:1 SET owner = user:1;
CREATE profile:1 SET account = account:1;

-- Baseline: direct record lookup works
SELECT owner, profiles FROM account:1;

-- Bug: WHERE clause on indexed field causes computed ref to return NONE
SELECT owner, profiles FROM account WHERE owner = user:1;
