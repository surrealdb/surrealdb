/**
[env]
namespace = true
database = true

[test]
reason = """
	A clause that reads the record by a name it computes (`type::field('ci' +
	'ty')`, `$this['ci' + 'ty']`), or hands the whole record to a builtin that
	reads its names (`object::keys($this)`), may read any component. Renaming
	or removing the components is refused while it is defined, naming the
	clause.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Cannot change the record id of `d1`: the field `dyn` reads fields of the record by names that cannot be determined, so it may read its component `city`"

[[test.results]]
error = "Cannot change the record id of `d1`: the field `dyn` reads fields of the record by names that cannot be determined, so it may read its component `city`"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Cannot change the record id of `d2`: the field `keyed` reads fields of the record by names that cannot be determined, so it may read its component `city`"

[[test.results]]
error = "Cannot change the record id of `d2`: the field `keyed` reads fields of the record by names that cannot be determined, so it may read its component `city`"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Cannot change the record id of `d3`: the field `keys` reads fields of the record by names that cannot be determined, so it may read its component `city`"

[[test.results]]
error = "Cannot change the record id of `d3`: the field `keys` reads fields of the record by names that cannot be determined, so it may read its component `city`"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Cannot change the record id of `d4`: the event `ev` reads fields of the record by names that cannot be determined, so it may read its component `city`"

[[test.results]]
error = "Cannot change the record id of `d4`: the event `ev` reads fields of the record by names that cannot be determined, so it may read its component `city`"
*/

DEFINE FIELD id ON d1 TYPE [city: string, time: datetime];
DEFINE FIELD dyn ON d1 COMPUTED type::field('ci' + 'ty');
ALTER FIELD id ON d1 TYPE [place: string, time: datetime];
REMOVE FIELD id ON d1;
DEFINE FIELD id ON d2 TYPE [city: string, time: datetime];
DEFINE FIELD keyed ON d2 COMPUTED $this['ci' + 'ty'];
ALTER FIELD id ON d2 TYPE [place: string, time: datetime];
REMOVE FIELD id ON d2;

-- The record handed whole to a builtin that reads its names.
DEFINE FIELD id ON d3 TYPE [city: string, time: datetime];
DEFINE FIELD keys ON d3 COMPUTED object::keys($this);
ALTER FIELD id ON d3 TYPE [place: string, time: datetime];
REMOVE FIELD id ON d3;
DEFINE FIELD id ON d4 TYPE [city: string, time: datetime];
DEFINE EVENT ev ON d4 THEN { CREATE audit SET keys = object::keys($value) };
ALTER FIELD id ON d4 TYPE [place: string, time: datetime];
REMOVE FIELD id ON d4;
