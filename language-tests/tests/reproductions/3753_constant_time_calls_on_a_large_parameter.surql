/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "Calls of constant cost on an array bound to a parameter answer from that array, and a statement that does not read the parameter does not pay for it. Each read of the parameter copies the array, a cost no result shows."
issue = 3753

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "1"

[[test.results]]
value = "0"

[[test.results]]
value = "50000"

[[test.results]]
value = "1249975000"

[[test.results]]
value = "1"

[[test.results]]
value = "NONE"

[[test.results]]
value = "50000"
*/

LET $small = [1];
LET $large = <array> 0..50000;
array::first($small);
array::first($large);
array::len($large);
math::sum($large);
RETURN 1;
LET $copy = $large;
array::len($copy);
