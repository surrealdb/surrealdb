/**
[env]
namespace = true
database = true

[test]
reason = """
	An event reads the record it fires for through `$after`, `$before` and
	`$value` as well as by bare field names, so `$after.city`, `$before.city`,
	`$value.city` and `$after['city']` each read the component `city` of the
	record id. The record id cannot drop or rename that name, nor can its `id`
	be removed, while such an event exists; once the event is removed it can.
"""

# `$after.city` in the condition.
[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Cannot change the record id of `reading`: the event `after_city` reads its component `city`"

[[test.results]]
error = "Cannot change the record id of `reading`: the event `after_city` reads its component `city`"

# `$before.city` in the body.
[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Cannot change the record id of `log`: the event `before_city` reads its component `city`"

# `$value.city` in the condition.
[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Cannot change the record id of `gauge`: the event `value_city` reads its component `city`"

# `$after['city']` in the body.
[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Cannot change the record id of `meter`: the event `indexed_city` reads its component `city`"

# Renaming a component no event reads is allowed, and so is renaming `city`
# once the event that reads it is removed.
[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"
*/

DEFINE FIELD id ON reading TYPE [city: string, time: datetime];
DEFINE EVENT after_city ON reading WHEN $after.city = 'London' THEN {};
ALTER FIELD id ON reading TYPE [place: string, time: datetime];
REMOVE FIELD id ON reading;

DEFINE FIELD id ON log TYPE [city: string, time: datetime];
DEFINE EVENT before_city ON log THEN { CREATE audit SET place = $before.city };
ALTER FIELD id ON log TYPE [place: string, time: datetime];

DEFINE FIELD id ON gauge TYPE [city: string, time: datetime];
DEFINE EVENT value_city ON gauge WHEN $value.city = 'London' THEN {};
ALTER FIELD id ON gauge TYPE [place: string, time: datetime];

DEFINE FIELD id ON meter TYPE [city: string, time: datetime];
DEFINE EVENT indexed_city ON meter THEN { CREATE audit SET place = $after['city'] };
ALTER FIELD id ON meter TYPE [place: string, time: datetime];

ALTER FIELD id ON reading TYPE [city: string, at: datetime];
REMOVE EVENT after_city ON reading;
ALTER FIELD id ON reading TYPE [place: string, at: datetime];
