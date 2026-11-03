/**
[env]
namespace = true
database = true

[test]
reason = "A union holding a named type tries its members in the order coercion selects one, unchanged before converting, and a member whose named type refuses the value hands it to the next"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ f: 5i32, id: t:1 }]"

[[test.results]]
value = "[{ f: 20f32, id: t:2 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Couldn't coerce value for field `p` of `u:3`: Expected `mood ('sad' | 'happy')` but found `'invalid'`"

[[test.results]]
error = "Couldn't coerce value for field `p` of `u:4`: Expected `posint (ASSERT $value > 0 failed)` but found `-1`"

[[test.results]]
value = "[{ id: u:5, p: ['happy', 2] }]"
*/

DEFINE TYPE capped AS i32 ASSERT $value < 10;
DEFINE FIELD f ON t TYPE f32 | capped;
CREATE t:1 SET f = <i32>5;
CREATE t:2 SET f = <i32>20;
DEFINE TYPE mood AS 'sad' | 'happy';
DEFINE TYPE posint AS int ASSERT $value > 0;
DEFINE FIELD p ON u TYPE [mood, posint];
CREATE u:3 SET p = ['invalid', -1];
CREATE u:4 SET p = ['sad', -1];
CREATE u:5 SET p = ['happy', 2];
