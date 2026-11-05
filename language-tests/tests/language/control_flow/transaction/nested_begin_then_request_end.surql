/**
[test]
reason = "A request that ends after a nested BEGIN reports the nested BEGIN and no \"Missing COMMIT statement\": every other statement of the block reports the failed transaction and the end adds no slot"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The query was not executed due to a failed transaction"

[[test.results]]
error = "Tried to start a transaction while another transaction was open"

[[test.results]]
error = "The query was not executed due to a failed transaction"

[env]
namespace = "test"
database = "test"

*/

BEGIN;
CREATE t:1;
BEGIN;
CREATE t:2;
