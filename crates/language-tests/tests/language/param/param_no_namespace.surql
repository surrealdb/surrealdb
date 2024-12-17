/**
[env]
namespace = false
database = false

[test]
reason = """
Using a param which does not exist without a namespace triggers an error.
However accessing a non existing param does cause an error.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "1"

[[test.results]]
error = "Specify a namespace to use"
*/
let $does_exit = 1;

$does_exit;

$does_not_exist;
