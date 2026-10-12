/**
[env]
namespace = true
database = true

[test]
reason = """
	A component name is a plain identifier: ASCII letters, digits and
	underscores, not starting with a digit, and neither NaN nor Infinity. A
	record's fields are handed to its events, views, RETURN images and field
	clauses under their raw names, which escape any other name, so a name that
	needs backticks is refused by DEFINE, DEFINE OVERWRITE and ALTER alike,
	before the type at the same position, and the definition is unchanged. A
	plain name written with backticks is accepted.
"""

[[test.results]]
error = "Cannot name the record id component `the city` of `a`: a component name must be a plain identifier of ASCII letters, digits and underscores that does not start with a digit and is not NaN or Infinity"

[[test.results]]
error = "Cannot name the record id component `a-b` of `a`: a component name must be a plain identifier of ASCII letters, digits and underscores that does not start with a digit and is not NaN or Infinity"

[[test.results]]
error = "Cannot name the record id component `café` of `a`: a component name must be a plain identifier of ASCII letters, digits and underscores that does not start with a digit and is not NaN or Infinity"

[[test.results]]
error = "Cannot name the record id component `1x` of `a`: a component name must be a plain identifier of ASCII letters, digits and underscores that does not start with a digit and is not NaN or Infinity"

[[test.results]]
error = "Cannot name the record id component `NaN` of `a`: a component name must be a plain identifier of ASCII letters, digits and underscores that does not start with a digit and is not NaN or Infinity"

[[test.results]]
error = "Cannot name the record id component `Infinity` of `a`: a component name must be a plain identifier of ASCII letters, digits and underscores that does not start with a digit and is not NaN or Infinity"

[[test.results]]
error = "Cannot name the record id component `x y` of `a`: a component name must be a plain identifier of ASCII letters, digits and underscores that does not start with a digit and is not NaN or Infinity"

[[test.results]]
value = "{ events: {  }, fields: {  }, indexes: {  }, lives: {  }, tables: {  } }"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ events: {  }, fields: { id: 'DEFINE FIELD id ON a TYPE [city: string, _t1: int] PERMISSIONS FULL' }, indexes: {  }, lives: {  }, tables: {  } }"

[[test.results]]
error = "Cannot name the record id component `the city` of `a`: a component name must be a plain identifier of ASCII letters, digits and underscores that does not start with a digit and is not NaN or Infinity"

[[test.results]]
error = "Cannot name the record id component `the city` of `a`: a component name must be a plain identifier of ASCII letters, digits and underscores that does not start with a digit and is not NaN or Infinity"

[[test.results]]
value = "{ events: {  }, fields: { id: 'DEFINE FIELD id ON a TYPE [city: string, _t1: int] PERMISSIONS FULL' }, indexes: {  }, lives: {  }, tables: {  } }"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Cannot name the record id component `a b` of `b`: a component name must be a plain identifier of ASCII letters, digits and underscores that does not start with a digit and is not NaN or Infinity"

[[test.results]]
value = "{ events: {  }, fields: { id: 'DEFINE FIELD id ON b TYPE string PERMISSIONS FULL' }, indexes: {  }, lives: {  }, tables: {  } }"
*/

DEFINE FIELD id ON a TYPE [`the city`: string];
DEFINE FIELD id ON a TYPE [`a-b`: string];
DEFINE FIELD id ON a TYPE [`café`: string];
DEFINE FIELD id ON a TYPE [`1x`: string];
DEFINE FIELD id ON a TYPE [`NaN`: string];
DEFINE FIELD id ON a TYPE [`Infinity`: string];
-- The name is judged before the type at its position: `float` is refused too.
DEFINE FIELD id ON a TYPE [ok: string, `x y`: float];
INFO FOR TABLE a;

-- Plain names, one of them escaped.
DEFINE FIELD id ON a TYPE [`city`: string, _t1: int];
INFO FOR TABLE a;
ALTER FIELD id ON a TYPE [`the city`: string, _t1: int];
DEFINE FIELD OVERWRITE id ON a TYPE [`the city`: string, _t1: int];
INFO FOR TABLE a;

-- Naming an id that had no names.
DEFINE FIELD id ON b TYPE string;
ALTER FIELD id ON b TYPE [`a b`: string];
INFO FOR TABLE b;
