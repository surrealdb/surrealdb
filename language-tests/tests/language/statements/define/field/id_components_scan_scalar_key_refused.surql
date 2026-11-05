/**
[env]
namespace = true
database = true

[test]
reason = "A record whose id is not an array cannot be given named components: the statement is refused, and the error names the record."

[[test.results]]
value = "[]"

[[test.results]]
error = "Cannot give `reading` the record id [city: string, time: datetime]: the existing record reading:5 does not match it (the record id is not an array). Copy the records into a table with the new record id instead"

[[test.results]]
value = "{ events: {  }, fields: {  }, indexes: {  }, lives: {  }, tables: {  } }"
*/

CREATE reading:5 SET temp = 1 RETURN NONE;

DEFINE FIELD id ON reading TYPE [city: string, time: datetime];

-- The refused statement defined nothing.
INFO FOR TABLE reading;
