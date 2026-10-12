/**
[env]
namespace = true
database = true

[test]
reason = "A datetime component holds only datetimes: a record id with a string at that position is refused, and the error names the component."

[[test.results]]
value = "[]"

[[test.results]]
error = "Cannot give `reading` the record id [city: string, time: datetime]: the existing record reading:['London', 'yesterday'] does not match it (`time` is not a datetime). Copy the records into a table with the new record id instead"

[[test.results]]
value = "{ events: {  }, fields: {  }, indexes: {  }, lives: {  }, tables: {  } }"
*/

CREATE reading:['London', 'yesterday'] SET temp = 1 RETURN NONE;

DEFINE FIELD id ON reading TYPE [city: string, time: datetime];

-- The refused statement defined nothing.
INFO FOR TABLE reading;
