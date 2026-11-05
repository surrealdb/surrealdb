/**
[test]
run = false
reason = "Setup only: rows with a decimal, a boolean and a NULL, read by collection_aggregates"
*/

CREATE vals:1 SET k = 1, label = 'ada', amount = 120.5f, exact = 1.25dec, flag = true RETURN NONE;
CREATE vals:2 SET k = 2, label = 'bob', amount = 50f, exact = 2.5dec, flag = false RETURN NONE;
CREATE vals:3 SET k = 3, label = 'cy', amount = NULL, exact = NULL, flag = NULL RETURN NONE;
CREATE vals:4 SET k = 4, label = 'dan', amount = 7.25f, exact = 3dec, flag = true RETURN NONE;
