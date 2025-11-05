/**
[test]
reason = "relating a record id with a strange name"

[[test.results]]
value = "[{ id: a:1 }, { id: a:2 }]"

[[test.results]]
value = "[{ id: ⟨-⟩:⟨-⟩, in: a:1, out: a:2 }]"

[[test.results]]
value = "[{ rel: [⟨-⟩:⟨-⟩] }]"

*/

CREATE a:1, a:2;
RELATE a:1->`-`:`-`->a:2;
select ->`-` as rel from a:1;
