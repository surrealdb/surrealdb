/**
[env]
namespace = true
database = true

[test]
reason = """
	A table whose id was never typed can hold a float or a decimal at an int
	position, which a named int component refuses. The way out is to copy the
	records into a new table whose id names its components from the start: the
	new table is empty, so defining it checks nothing, and an explicit id
	written to a table with named components has each int element converted to
	an int, so `5` and `5.0` meet as one id and the second is refused instead
	of being silently kept apart. A table whose id is typed but not named
	converts nothing (it keeps `5.0` as a float), so the names come
	before the copy, not after it.
"""

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
error = "Cannot give `legacy` the record id [city: string, n: int]: the existing record legacy:['London', 5f] does not match it (`n` is a float; an int component holds only ints). Copy the records into a table with the new record id instead"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
error = "Database record `fixed:['London', 5]` already exists"

[[test.results]]
value = "[[5, true, 1]]"
*/

CREATE legacy:['London', 5] SET temp = 1 RETURN NONE;
CREATE legacy:['London', 5.0] SET temp = 2 RETURN NONE;
DEFINE FIELD id ON legacy TYPE [city: string, n: int];

-- The copy goes into a new table whose id names its components first.
DEFINE FIELD id ON fixed TYPE [city: string, n: int];
CREATE fixed:['London', 5] SET temp = 1 RETURN NONE;
CREATE fixed:['London', 5.0] SET temp = 2 RETURN NONE;

-- One record, keyed by the int.
SELECT VALUE [n, type::is_int(n), temp] FROM fixed;
