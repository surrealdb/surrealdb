/**
[test]
reason = "Example from https://surrealdb.com/docs/surrealql/datamodel/references on ON DELETE CASCADE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ comments: [], id: person:one }]"

[[test.results]]
value = "[{ author: person:one, id: comment:one, text: '5/10 for this blog post. The problems I have with it are...' }]"

[[test.results]]
value = "[{ author: person:one, id: comment:two, text: 'WOW! I never knew you could cut a rope with an arrow.' }]"

[[test.results]]
value = "[{ author: person:one, id: comment:one, text: '5/10 for this blog post. The problems I have with it are...' }, { author: person:one, id: comment:two, text: 'WOW! I never knew you could cut a rope with an arrow.' }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[env.capabilities]
allow-experimental = ["record_references"]

*/

DEFINE FIELD author ON comment TYPE record<person> REFERENCE ON DELETE CASCADE;
DEFINE FIELD comments ON person COMPUTED <~comment;

CREATE person:one;
CREATE comment:one SET author = person:one, text = "5/10 for this blog post. The problems I have with it are...";
CREATE comment:two SET author = person:one, text = "WOW! I never knew you could cut a rope with an arrow.";

-- Show all the details of comments for 'person:one'
person:one.comments.*.*;
DELETE person:one;
-- Comments no longer exist
SELECT * FROM comment;