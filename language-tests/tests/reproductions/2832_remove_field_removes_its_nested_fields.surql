/**
[test]
reason = "REMOVE FIELD of a field with nested definitions removes those definitions with it, and on a SCHEMAFULL table with rows (compatibility level 4) the stored values go too, so the object no longer reads back and writing it is refused."
issue = 2832

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
value = "[{ contact_info: { email: 'e', mobile: '1' }, id: contact:1, name: 'p' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "['contact_infox', 'name']"

[[test.results]]
value = "[{ id: contact:1, name: 'p' }]"

[[test.results]]
error = "Found field 'contact_info', but no such field exists for table 'contact'"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

*/

DEFINE TABLE contact SCHEMAFULL;
DEFINE FIELD name ON contact TYPE string;
DEFINE FIELD contact_info ON contact TYPE option<object>;
DEFINE FIELD contact_info.mobile ON contact TYPE option<string>;
DEFINE FIELD contact_info.email ON contact TYPE option<string>;
DEFINE FIELD contact_infox ON contact TYPE option<string>;
CREATE contact:1 CONTENT { name: "p", contact_info: { mobile: "1", email: "e" } };
REMOVE FIELD contact_info ON contact;
object::keys((INFO FOR TABLE contact).fields);
SELECT * FROM contact;
CREATE contact:2 CONTENT { name: "p", contact_info: { mobile: "1", email: "e" } };
-- On a table with no rows the definitions go just the same.
DEFINE TABLE empty SCHEMAFULL;
DEFINE FIELD a ON empty TYPE option<object>;
DEFINE FIELD a.b ON empty TYPE option<string>;
REMOVE FIELD a ON empty;
object::keys((INFO FOR TABLE empty).fields);
