/**
[test]
issue = 5902
reason = "Check that a new reference is added when a record is updated"

[[test.results]]
value = "NONE"
[[test.results]]
value = "NONE"


[[test.results]]
value = "[{ comments: [], id: person:one }]"
[[test.results]]
value = "[{ comments: [], id: person:two }]"


[[test.results]]
value = "[{ author: person:one, id: comment:1, text: 'one' }]"
[[test.results]]
value = "[{ author: person:one, id: comment:2, text: 'two' }]"


[[test.results]]
value = "[{ author: person:one, id: comment:1, text: 'one' }, { author: person:one, id: comment:2, text: 'two' }]"
[[test.results]]
value = "[]"


[[test.results]]
value = "[{ author: person:two, id: comment:1, text: 'one' }]"


[[test.results]]
value = "[{ author: person:one, id: comment:2, text: 'two' }]"
[[test.results]]
value = "[{ author: person:two, id: comment:1, text: 'one' }]"

[env.capabilities]
allow-experimental = ["record_references"]

*/
DEFINE FIELD author ON comment TYPE record<person> REFERENCE ON DELETE CASCADE;
DEFINE FIELD comments ON person COMPUTED <~comment;

CREATE person:one;
CREATE person:two;

CREATE comment SET id = comment:1, author = person:one, text = "one";
CREATE comment SET id = comment:2, author = person:one, text = "two";

person:one.comments.*.*;
person:two.comments.*.*;

UPDATE comment:1 SET author = person:two;

person:one.comments.*.*;
person:two.comments.*.*;