/**
[env]
namespace = true
database = true

[test]
reason = "A fractional decimal assigned to a field whose kind has an int member is an error, not a panic. A union kind coerces to the first member that reports it can take the value, so that report has to match what the coercion then does."

[[test.results]]
value = "NONE"

[[test.results]]
error = "Couldn't coerce value for field `n` of `t:1`: Expected `none | int` but found `1.5dec`"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Couldn't coerce value for field `u` of `v:1`: Expected `int | string` but found `1.5dec`"

[[test.results]]
error = "Could not cast into `int | bool` using input `1.5dec`"

[[test.results]]
value = "'1.5'"
*/

DEFINE FIELD n ON t TYPE option<int>;
CREATE t:1 SET n = 1.5dec;
DEFINE FIELD u ON v TYPE int | string;
CREATE v:1 SET u = 1.5dec;
<int | bool>1.5dec;
-- The int member refuses the value, so the string member takes it
<int | string>1.5dec;
