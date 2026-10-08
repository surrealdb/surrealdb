/**
[env]
imports = ["reproductions/a_permission_holding_a_write_names_the_failed_check_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "account:alice" }

[test]
reason = "A table or field permission that holds a statement writing data fails a read naming the check that failed, as a scan enforcing it names it. A record read by dereferencing it fails its table permission with the refusal alone and names a failed field permission; a predicate that raises when evaluated fails with the error it raises."

[[test.results]]
error = "Failed to check permission: A PERMISSIONS clause cannot contain a statement that modifies data"

[[test.results]]
error = "Failed to check permission: A PERMISSIONS clause cannot contain a statement that modifies data"

[[test.results]]
error = "A PERMISSIONS clause cannot contain a statement that modifies data"

[[test.results]]
error = "Failed to check field permission: A PERMISSIONS clause cannot contain a statement that modifies data"

[[test.results]]
error = "Failed to check field permission: A PERMISSIONS clause cannot contain a statement that modifies data"

[[test.results]]
error = "A PERMISSIONS clause cannot contain a statement that modifies data"

[[test.results]]
error = "A PERMISSIONS clause cannot contain a statement that modifies data"

[[test.results]]
error = "Failed to check field permission: A PERMISSIONS clause cannot contain a statement that modifies data"

[[test.results]]
error = "Failed to check field permission: A PERMISSIONS clause cannot contain a statement that modifies data"

[[test.results]]
error = "Failed to check field permission: A PERMISSIONS clause cannot contain a statement that modifies data"

[[test.results]]
error = "A PERMISSIONS clause cannot contain a statement that modifies data"

[[test.results]]
error = "An error occurred: boom"

[[test.results]]
error = "An error occurred: boom"

[[test.results]]
error = "Incorrect arguments for function math::sqrt(). Argument 1 was the wrong type. Expected `number` but found `'a'`"

[[test.results]]
error = "Incorrect arguments for function math::sqrt(). Argument 1 was the wrong type. Expected `number` but found `'a'`"

*/
SELECT * FROM d;
SELECT * FROM d:1;
RETURN d:1.*;
SELECT * FROM g;
SELECT f FROM g:1;
SELECT * FROM h;
UPDATE d SET z = 1;
RETURN g:1.*;
RETURN g:1.f;
RETURN g:1.*.f;
RETURN h:1.*;
RETURN r1:1.*;
SELECT * FROM r1;
RETURN r3:1.*;
SELECT * FROM r3;
