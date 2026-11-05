/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
	In an event, `$input` is the data the statement wrote, which names a
	component written through SET or CONTENT. An event that reads a component
	through `$input` blocks renaming or removing it, as one reading it
	through `$after` does; the refusal changes nothing, and the event keeps
	logging the component.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ city: 'f', id: reading:['f', 1], n: 1 }]"

[[test.results]]
error = "Cannot change the record id of `reading`: the event `e` reads its component `city`"

[[test.results]]
error = "Cannot change the record id of `reading`: the event `e` reads its component `city`"

[[test.results]]
value = "[{ city: 'g', id: reading:['g', 2], n: 2 }]"

[[test.results]]
value = "['f', 'g']"

*/

DEFINE FIELD id ON reading TYPE [city: string, n: int];
DEFINE EVENT e ON reading THEN { CREATE log SET v = $input.city };
CREATE reading SET city = 'f', n = 1;
ALTER FIELD id ON reading TYPE [place: string, n: int];
REMOVE FIELD id ON reading;
CREATE reading SET city = 'g', n = 2;
SELECT VALUE v FROM log ORDER BY v;
