/**
[env]
namespace = true
database = true

[test]
reason = "A record id whose key holds a named type is judged against that type's body and checks; a key that is a named type alone is refused when the field is defined"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Cannot use the `percent` type on the `id` field, as that's not a valid record id key."

[[test.results]]
value = "NONE"

[[test.results]]
error = "Couldn't coerce value for field `id` of `r:['a', 101]`: Expected `percent (ASSERT $value <= 100 failed)` but found `101`"

[[test.results]]
error = "Couldn't coerce value for field `id` of `r:['a', 'x']`: Expected `percent (int)` but found `'x'`"

[[test.results]]
value = "[{ id: r:['a', 99] }]"
*/

DEFINE TYPE percent AS int ASSERT $value <= 100;
DEFINE FIELD id ON t TYPE percent;
DEFINE FIELD id ON r TYPE [string, percent];
CREATE r:['a', 101];
CREATE r:['a', 'x'];
CREATE r:['a', 99];
