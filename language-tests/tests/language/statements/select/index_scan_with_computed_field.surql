/**
[test]
reason = "Test IndexScan with computed field."

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
value = "[{ id: account:1, owner: user:1, profiles:[] }]"

[[test.results]]
value = "[{ owner: user:1, profiles:[] }]"

[[test.results]]
value = "[{ owner: user:1, profiles:[] }]"

[[test.results]]
value = "[{ id: profile:1, account: account:1 }]"

[[test.results]]
value = "[{ owner: user:1, profiles:[profile:1] }]"

[[test.results]]
value = "[{ owner: user:1, profiles:[profile:1] }]"

[[test.results]]
value = "[{ id: profile:2, account: account:1 }]"

[[test.results]]
value = "[{ owner: user:1, profiles:[profile:1, profile:2] }]"

[[test.results]]
value = "[{ owner: user:1, profiles:[profile:1, profile:2] }]"

*/

DEFINE TABLE OVERWRITE account SCHEMAFULL PERMISSIONS NONE;
DEFINE FIELD OVERWRITE profiles ON TABLE account COMPUTED <~(profile FIELD account);

DEFINE FIELD OVERWRITE owner ON TABLE account TYPE record<user>;
DEFINE INDEX OVERWRITE owner ON TABLE account FIELDS owner;

DEFINE TABLE OVERWRITE profile SCHEMAFULL PERMISSIONS NONE;
DEFINE FIELD OVERWRITE account ON TABLE profile TYPE record<account> REFERENCE ON DELETE REJECT;

CREATE account:1 SET owner = user:1;
SELECT owner, profiles FROM account;
SELECT owner, profiles FROM account WHERE owner = user:1;

CREATE profile:1 SET account = account:1;
SELECT owner, profiles FROM account;
SELECT owner, profiles FROM account WHERE owner = user:1;

CREATE profile:2 SET account = account:1;
SELECT owner, profiles FROM account;
SELECT owner, profiles FROM account WHERE owner = user:1;
