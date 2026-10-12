/**
[env]
namespace = true
database = true

[test]
reason = """
	A datetime component holds the years 0000 to 9999. A record id holding a
	later datetime is stored as text that is not read back as a datetime, so the
	scan of the existing records cannot read that record's id: naming the
	components is refused, the error says why, and the table is unchanged.
"""

[[test.results]]
value = "[]"

[[test.results]]
error = "Cannot give `reading` the record id [city: string, time: datetime]: the existing record in `reading` whose record id cannot be read does not match it (a record id holding a datetime outside the years 0000 to 9999 cannot be read back). Copy the records into a table with the new record id instead"

[[test.results]]
value = "{ events: {  }, fields: {  }, indexes: {  }, lives: {  }, tables: {  } }"
*/

-- 253402300800 seconds after the epoch is the first instant of the year 10000.
CREATE reading:['London', time::from_secs(253402300800)] SET temp = 1 RETURN NONE;

DEFINE FIELD id ON reading TYPE [city: string, time: datetime];

INFO FOR TABLE reading;
