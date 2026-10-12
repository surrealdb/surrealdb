/**
[env]
namespace = true
database = true

[test]
reason = "An int component holds only ints: a decimal that equals a whole number is refused all the same, and the error says so."

[[test.results]]
value = "[]"

[[test.results]]
error = "Cannot give `reading` the record id [city: string, n: int]: the existing record reading:['London', 5dec] does not match it (`n` is a decimal; an int component holds only ints). Copy the records into a table with the new record id instead"

[[test.results]]
value = "{ events: {  }, fields: {  }, indexes: {  }, lives: {  }, tables: {  } }"
*/

CREATE reading:['London', 5dec] SET temp = 1 RETURN NONE;

DEFINE FIELD id ON reading TYPE [city: string, n: int];

-- The refused statement defined nothing.
INFO FOR TABLE reading;
