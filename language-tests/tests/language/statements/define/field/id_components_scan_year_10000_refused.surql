/**
[env]
namespace = true
database = true

[test]
reason = """
	A datetime component holds the years 0000 to 9999. A record id holding a
	later datetime is read back, but its datetime lies outside that range: the
	scan of the existing records finds a record that does not fit, naming the
	components is refused, the error names the record and says why, and the
	table is unchanged.
"""

[[test.results]]
value = "[]"

[[test.results]]
error = "Cannot give `reading` the record id [city: string, time: datetime]: the existing record reading:['London', d'+10000-01-01T00:00:00Z'] does not match it (`time` is a datetime outside the years 0000 to 9999). Copy the records into a table with the new record id instead"

[[test.results]]
value = "{ events: {  }, fields: {  }, indexes: {  }, lives: {  }, tables: {  } }"
*/

-- 253402300800 seconds after the epoch is the first instant of the year 10000.
CREATE reading:['London', time::from_secs(253402300800)] SET temp = 1 RETURN NONE;

DEFINE FIELD id ON reading TYPE [city: string, time: datetime];

INFO FOR TABLE reading;
