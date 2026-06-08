/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "DEFINE INDEX FIELDS accepts parameterized bracket indices and substitutes them to literals before storing on disk, parallel to DEFINE FIELD."

# 0: LET $idx = 0
[[test.results]]
value = "NONE"

# 1: DEFINE TABLE thing
[[test.results]]
value = "NONE"

# 2: DEFINE INDEX i_data ON thing FIELDS data[$idx]
[[test.results]]
value = "NONE"

# 3: INFO FOR TABLE thing — index is stored with the substituted literal index.
[[test.results]]
match = "$result.indexes.i_data = /DEFINE INDEX i_data ON thing FIELDS data\\[0\\]/"

# 4: REMOVE INDEX i_data ON thing
[[test.results]]
value = "NONE"
*/

LET $idx = 0;
DEFINE TABLE thing;
DEFINE INDEX i_data ON thing FIELDS data[$idx];
INFO FOR TABLE thing;
REMOVE INDEX i_data ON thing;
