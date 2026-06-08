/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "DEFINE FIELD duplicate-detection compares the resolved field name (after substituting parameterized indices) — a second DEFINE FIELD with a parameterized index that resolves to the same key must error with FdAlreadyExists rather than silently overwrite."

# 0: LET $idx = 0
[[test.results]]
value = "NONE"

# 1: DEFINE FIELD data[$idx] ON TABLE thing TYPE number
[[test.results]]
value = "NONE"

# 2: DEFINE FIELD data[$idx] ON TABLE thing TYPE string — must reject (not silently overwrite)
[[test.results]]
match = "$error = /already exists/"
error = true

# 3: INFO FOR TABLE thing — confirms the original TYPE number is intact
[[test.results]]
match = "$result.fields['data[0]'] = /DEFINE FIELD data\\[0\\].*TYPE number/"
*/

LET $idx = 0;
DEFINE FIELD data[$idx] ON TABLE thing TYPE number;
DEFINE FIELD data[$idx] ON TABLE thing TYPE string;
INFO FOR TABLE thing;
