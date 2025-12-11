/**
[test]
reason = "Inside fn prepare_computed() for a value that is not a Table or RecordID, the next arm of the match statement will check if the statement is a SELECT. If it is not a SELECT statement, it will only then check to see if it is an object and use fn rid() in that case to look for an id, followed by fn prepare_record_id."

[[test.results]]
value = "NONE"

[[test.results]]
value = "false"

[[test.results]]
value = "[{ enabled: false, id: a:1 }]"

**/

LET $a = CREATE ONLY a:1 SET enabled = true;
// Has id field but is not a record. However, check for SELECT happens first so no lookup takes place.
SELECT VALUE enabled FROM ONLY { id: a:1, enabled: false };
// Not a select in this case so proceeds to look for ID, uses that to update the record
UPDATE $a SET enabled = false;