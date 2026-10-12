/**
[env]
namespace = true
database = true

[test]
reason = """
	Records whose ids hold a string, an int, a bool, a duration, a record of
	the named table and a uuid fit a structure of those types, and so does a
	record of any table at a component that names no table.
"""

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[1, 2]"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"
*/

CREATE reading:['London', 5, true, 2h, person:alice, u'018f0a52-1c0f-7a3b-9c4d-0123456789ab'] SET temp = 1 RETURN NONE;
CREATE reading:['Paris', -3, false, 90m, person:bob, u'018f0a52-1c0f-7a3b-9c4d-0123456789ac'] SET temp = 2 RETURN NONE;

DEFINE FIELD id ON reading TYPE [city: string, n: int, ok: bool, span: duration, who: record<person>, uid: uuid];

SELECT VALUE temp FROM reading ORDER BY temp;

-- A record component that names no table admits any.
CREATE assigned:[person:alice, 1] SET temp = 1 RETURN NONE;
DEFINE FIELD id ON assigned TYPE [who: record, n: int];
