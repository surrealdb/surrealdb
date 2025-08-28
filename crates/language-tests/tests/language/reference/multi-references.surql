/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: b:1, ref: a:1 }]"

[[test.results]]
value = "[{ id: c:1, ref: a:1 }]"

[[test.results]]
value = "[{ id: a:1, refs: [b:1, c:1] }]"

[env.capabilities]
allow-experimental = ["record_references"]

*/
DEFINE FIELD refs ON a COMPUTED <~(b, c);
DEFINE FIELD ref ON b TYPE record<a> REFERENCE;
DEFINE FIELD ref ON c TYPE record<a> REFERENCE;

CREATE b:1 SET ref = a:1;
CREATE c:1 SET ref = a:1;
CREATE a:1;
