/**
[env]
namespace = true
database = true

[test]
reason = """
	`$parent.city` and a destructure of the record (`$this.{city}`,
	`$value.{time}`) read the components they name. They are readers of those
	names, reported as such, and of no other name.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Cannot change the record id of `q1`: the field `p` reads its component `city`"

[[test.results]]
error = "Cannot change the record id of `q1`: the field `p` reads its component `city`"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Cannot change the record id of `q2`: the field `d` reads its component `city`"

[[test.results]]
error = "Cannot change the record id of `q2`: the field `d` reads its component `city`"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Cannot change the record id of `q3`: the event `ev` reads its component `time`"
*/

DEFINE FIELD id ON q1 TYPE [city: string, time: datetime];
DEFINE FIELD p ON q1 COMPUTED $parent.city;
ALTER FIELD id ON q1 TYPE [place: string, time: datetime];
REMOVE FIELD id ON q1;
DEFINE FIELD id ON q2 TYPE [city: string, time: datetime];
DEFINE FIELD d ON q2 COMPUTED $this.{city}.city;
ALTER FIELD id ON q2 TYPE [place: string, time: datetime];
REMOVE FIELD id ON q2;

-- An event that destructures `time` out of its record reads `time` alone:
-- renaming `city` is accepted, and removing every name names `time`.
DEFINE FIELD id ON q3 TYPE [city: string, time: datetime];
DEFINE EVENT ev ON q3 THEN { CREATE audit SET t = $value.{time}.time };
ALTER FIELD id ON q3 TYPE [place: string, time: datetime];
REMOVE FIELD id ON q3;
