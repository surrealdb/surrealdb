/**
[test]
reason = "Test ?= operator"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "false"

[[test.results]]
value = "false"

[[test.results]]
value = "false"

[[test.results]]
value = "false"


*/

-- all true
[1,2] ?= 1;
[1, [1,2]] ?= [1,2];
{1,2} ?= 1;
{1, {1,2}} ?= {1,2};

-- all false
[1,2] ?= 3;
[1, [1,2]] ?= [1,3];
{1,2} ?= 3;
{1, {1,2}} ?= {1,3};