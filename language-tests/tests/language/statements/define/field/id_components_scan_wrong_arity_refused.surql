/**
[env]
namespace = true
database = true

[test]
reason = "A record id with a different number of elements than the structure has is refused, and the error says how many it has and how many are expected."

[[test.results]]
value = "[]"

[[test.results]]
error = "Cannot give `reading` the record id [city: string, time: datetime]: the existing record reading:['London'] does not match it (the record id has 1 element, but 2 are expected). Copy the records into a table with the new record id instead"

[[test.results]]
value = "{ events: {  }, fields: {  }, indexes: {  }, lives: {  }, tables: {  } }"
*/

CREATE reading:['London'] SET temp = 1 RETURN NONE;

DEFINE FIELD id ON reading TYPE [city: string, time: datetime];

-- The refused statement defined nothing.
INFO FOR TABLE reading;
