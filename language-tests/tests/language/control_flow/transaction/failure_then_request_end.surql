/**
[test]
reason = "A request that ends after a failure inside a block reports the failure and no \"Missing COMMIT statement\": the statements before the failure report the failed transaction, those after it the cancelled one, and the end adds no slot"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The query was not executed due to a failed transaction"

[[test.results]]
error = "An error occurred: x"

[[test.results]]
error = "The query was not executed due to a cancelled transaction"

[env]
namespace = "test"
database = "test"

*/

BEGIN;
CREATE t:1;
THROW "x";
CREATE t:2;
