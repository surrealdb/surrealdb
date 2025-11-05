/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The param '$example' already exists"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The param '$example' does not exist"

[[test.results]]
value = "NONE"

*/

DEFINE PARAM $example VALUE 123;
DEFINE PARAM IF NOT EXISTS $example VALUE 123;
DEFINE PARAM OVERWRITE $example VALUE 123;
DEFINE PARAM $example VALUE 123;
REMOVE PARAM IF EXISTS $example;
REMOVE PARAM $example;
REMOVE PARAM IF EXISTS $example;
