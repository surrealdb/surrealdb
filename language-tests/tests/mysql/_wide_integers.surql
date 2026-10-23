/**
[test]
run = false
*/

CREATE ledger:1 SET amount = 5 RETURN NONE;
CREATE ledger:2 SET amount = -5 RETURN NONE;
CREATE ledger:3 SET amount = 9223372036854775807 RETURN NONE;
CREATE ledger:4 SET amount = -9223372036854775807 - 1 RETURN NONE;
CREATE ledger:5 SET amount = 0 RETURN NONE;
CREATE ledger:6 SET amount = NULL RETURN NONE;
