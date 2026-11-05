/**
[env]
namespace = true
database = true

[test]
reason = "A record component names the tables it admits: a record id holding a record of another table at that position is refused."

[[test.results]]
value = "[]"

[[test.results]]
error = "Cannot give `reading` the record id [owner: record<person>, time: datetime]: the existing record reading:[other:1, d'2024-01-01T00:00:00Z'] does not match it (`owner` is a record of another table). Copy the records into a table with the new record id instead"

[[test.results]]
value = "{ events: {  }, fields: {  }, indexes: {  }, lives: {  }, tables: {  } }"
*/

CREATE reading:[other:1, d'2024-01-01T00:00:00Z'] SET temp = 1 RETURN NONE;

DEFINE FIELD id ON reading TYPE [owner: record<person>, time: datetime];

-- The refused statement defined nothing.
INFO FOR TABLE reading;
