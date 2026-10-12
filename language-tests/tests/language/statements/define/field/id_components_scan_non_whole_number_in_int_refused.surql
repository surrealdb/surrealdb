/**
[env]
namespace = true
database = true

[test]
reason = "An int component holds only ints: a record id with 5.5 at that position is refused, and the error names the component."

[[test.results]]
value = "[]"

[[test.results]]
error = "Cannot give `reading` the record id [city: string, n: int]: the existing record reading:['London', 5.5f] does not match it (`n` is not an int). Copy the records into a table with the new record id instead"

[[test.results]]
value = "{ events: {  }, fields: {  }, indexes: {  }, lives: {  }, tables: {  } }"
*/

CREATE reading:['London', 5.5] SET temp = 1 RETURN NONE;

DEFINE FIELD id ON reading TYPE [city: string, n: int];

-- The refused statement defined nothing.
INFO FOR TABLE reading;
