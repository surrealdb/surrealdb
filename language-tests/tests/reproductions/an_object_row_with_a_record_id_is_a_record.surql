/**
[env]
namespace = true
database = true

[test]
reason = """
`@` names the identity of the row a statement iterates, and a row is a record
when it is an object whose `id` is a record id
(`the_at_root_names_a_record_identity.surql`). An object literal written in
`FROM`, held in a parameter, or reached through an array nested in a written
array -- which contributes its elements as rows -- is such a row, so `@` names
its `id`.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "['t:5']"

[[test.results]]
value = "['t:5']"

[[test.results]]
value = "['t:5']"

*/
LET $held = [{ id: t:5 }];
SELECT VALUE @.to_string() FROM [{ id: t:5 }];
SELECT VALUE @.to_string() FROM [[{ id: t:5 }]];
SELECT VALUE @.to_string() FROM $held;
