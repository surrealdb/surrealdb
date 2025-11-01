/**
[test]
reason = "Test transaction error handling with THROW statements and RETURN behavior"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The query was not executed due to a failed transaction"

[[test.results]]
error = "The query was not executed due to a failed transaction"

[[test.results]]
error = "An error occurred: Some error"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The query was not executed due to a failed transaction"

[[test.results]]
error = "The query was not executed due to a failed transaction"

[[test.results]]
error = "An error occurred: Some error"

[[test.results]]
error = "The query was not executed due to a cancelled transaction"

[env]
namespace = "test"
database = "test"

*/

-- Test 1: Transaction with THROW only
BEGIN;
LET $one = CREATE test; -- We see a transaction error here (The query was not executed due to a failed transaction)
LET $two = CREATE test; -- We see a transaction error here (The query was not executed due to a failed transaction)
THROW 'Some error'; -- This is the error we see in the results list (An error occurred: Some error)
COMMIT;

-- Test 2: Transaction with THROW followed by RETURN
BEGIN;
LET $one = CREATE test; -- We see a transaction error here (The query was not executed due to a failed transaction)
LET $two = CREATE test; -- We see a transaction error here (The query was not executed due to a failed transaction)
THROW 'Some error'; -- This is the error we see in the results list (An error occurred: Some error)
RETURN [$one, $two]; -- We see a transaction error here (The query was not executed due to a failed transaction)
COMMIT;
